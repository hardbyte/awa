// River portable benchmark adapter.
//
// Runs standardised benchmark scenarios and outputs JSON results.
// Env: DATABASE_URL (required), SCENARIO, JOB_COUNT, WORKER_COUNT, LATENCY_ITERATIONS
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
)

// ── Job type ──────────────────────────────────────────────────────

type BenchArgs struct {
	Seq int64 `json:"seq"`
}

func (BenchArgs) Kind() string { return "bench_job" }

type BenchWorker struct {
	river.WorkerDefaults[BenchArgs]
}

func (w *BenchWorker) Work(ctx context.Context, job *river.Job[BenchArgs]) error {
	return nil
}

// ── Chaos job type (for chaos tests) ─────────────────────────────

type ChaosArgs struct {
	Seq int64 `json:"seq"`
}

func (ChaosArgs) Kind() string { return "chaos_job" }

type ChaosWorker struct {
	river.WorkerDefaults[ChaosArgs]
	JobDurationMs int
}

func (w *ChaosWorker) Work(ctx context.Context, job *river.Job[ChaosArgs]) error {
	time.Sleep(time.Duration(w.JobDurationMs) * time.Millisecond)
	return nil
}

// ── Helpers ───────────────────────────────────────────────────────

func databaseURL() string {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		log.Fatal("DATABASE_URL must be set")
	}
	return url
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) int {
	v, err := strconv.Atoi(envOrDefault(key, strconv.Itoa(def)))
	if err != nil {
		log.Fatalf("%s must be an integer: %v", key, err)
	}
	return v
}

func readProducerRate(def int) int {
	path := os.Getenv("PRODUCER_RATE_CONTROL_FILE")
	if path == "" {
		return def
	}
	buf, err := os.ReadFile(path)
	if err != nil {
		return def
	}
	value, err := strconv.ParseFloat(strings.TrimSpace(string(buf)), 64)
	if err != nil {
		return def
	}
	if value < 0 {
		return 0
	}
	return int(value)
}

func mustPool(ctx context.Context) *pgxpool.Pool {
	poolConfig, err := pgxpool.ParseConfig(databaseURL())
	if err != nil {
		log.Fatalf("Failed to parse database URL: %v", err)
	}
	poolConfig.MaxConns = int32(envInt("MAX_CONNECTIONS", 20))
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	return pool
}

func migrate(ctx context.Context, pool *pgxpool.Pool) {
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), nil)
	if err != nil {
		log.Fatalf("Failed to create River migrator: %v", err)
	}

	_, err = migrator.Migrate(ctx, rivermigrate.DirectionUp, nil)
	if err != nil {
		log.Fatalf("Failed to run River migrations: %v", err)
	}
}

func cleanJobs(ctx context.Context, pool *pgxpool.Pool) {
	_, err := pool.Exec(ctx, "DELETE FROM river_job")
	if err != nil {
		log.Fatalf("Failed to clean river_job: %v", err)
	}
}

type stateCount struct {
	State string `json:"state"`
	Count int64  `json:"count"`
}

func countByState(ctx context.Context, pool *pgxpool.Pool) map[string]int64 {
	rows, err := pool.Query(ctx, "SELECT state::text, count(*)::bigint FROM river_job GROUP BY state")
	if err != nil {
		log.Fatalf("Failed to query state counts: %v", err)
	}
	defer rows.Close()

	counts := make(map[string]int64)
	for rows.Next() {
		var state string
		var count int64
		if err := rows.Scan(&state, &count); err != nil {
			log.Fatalf("Failed to scan row: %v", err)
		}
		counts[state] = count
	}
	return counts
}

func waitForCompletion(ctx context.Context, pool *pgxpool.Pool, expected int64, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for {
		var completed int64
		err := pool.QueryRow(ctx,
			"SELECT count(*) FROM river_job WHERE state = 'completed'",
		).Scan(&completed)
		if err != nil {
			log.Fatalf("Failed to count completed: %v", err)
		}
		if completed >= expected {
			return
		}
		if time.Now().After(deadline) {
			counts := countByState(ctx, pool)
			log.Fatalf("Timeout after %v: %d/%d completed, states: %v", timeout, completed, expected, counts)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// ── Result types ──────────────────────────────────────────────────

type BenchmarkResult struct {
	System   string          `json:"system"`
	Scenario string          `json:"scenario"`
	Config   json.RawMessage `json:"config"`
	Results  json.RawMessage `json:"results"`
}

func jsonRaw(v interface{}) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		log.Fatalf("JSON marshal error: %v", err)
	}
	return b
}

// ── Scenarios ─────────────────────────────────────────────────────

func scenarioEnqueueThroughput(ctx context.Context, pool *pgxpool.Pool, jobCount int) BenchmarkResult {
	cleanJobs(ctx, pool)

	// We use InsertManyFast for bulk insert (COPY protocol)
	workers := river.NewWorkers()
	river.AddWorker(workers, &BenchWorker{})

	// Create insert-only client (no queues needed)
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Workers: workers,
	})
	if err != nil {
		log.Fatalf("Failed to create insert client: %v", err)
	}

	batchSize := 500
	start := time.Now()
	for i := 0; i < jobCount; i += batchSize {
		end := i + batchSize
		if end > jobCount {
			end = jobCount
		}
		params := make([]river.InsertManyParams, 0, end-i)
		for j := i; j < end; j++ {
			params = append(params, river.InsertManyParams{
				Args: BenchArgs{Seq: int64(j)},
			})
		}
		_, err := client.InsertManyFast(ctx, params)
		if err != nil {
			log.Fatalf("InsertManyFast failed: %v", err)
		}
	}
	elapsed := time.Since(start)

	jobsPerSec := float64(jobCount) / elapsed.Seconds()
	cleanJobs(ctx, pool)

	return BenchmarkResult{
		System:   "river",
		Scenario: "enqueue_throughput",
		Config:   jsonRaw(map[string]int{"job_count": jobCount}),
		Results: jsonRaw(map[string]interface{}{
			"duration_ms":  elapsed.Milliseconds(),
			"jobs_per_sec": jobsPerSec,
		}),
	}
}

func scenarioWorkerThroughput(ctx context.Context, pool *pgxpool.Pool, jobCount int, workerCount int) BenchmarkResult {
	cleanJobs(ctx, pool)

	// Pre-enqueue all jobs using insert-only client
	workers := river.NewWorkers()
	river.AddWorker(workers, &BenchWorker{})

	insertClient, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Workers: workers,
	})
	if err != nil {
		log.Fatalf("Failed to create insert client: %v", err)
	}

	batchSize := 500
	for i := 0; i < jobCount; i += batchSize {
		end := i + batchSize
		if end > jobCount {
			end = jobCount
		}
		params := make([]river.InsertManyParams, 0, end-i)
		for j := i; j < end; j++ {
			params = append(params, river.InsertManyParams{
				Args: BenchArgs{Seq: int64(j)},
			})
		}
		_, err := insertClient.InsertManyFast(ctx, params)
		if err != nil {
			log.Fatalf("InsertManyFast failed: %v", err)
		}
	}

	// Now start the working client and measure drain time
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: workerCount},
		},
		Workers:           workers,
		JobTimeout:        -1,
		FetchCooldown:     50 * time.Millisecond,
		FetchPollInterval: 50 * time.Millisecond,
	})
	if err != nil {
		log.Fatalf("Failed to create worker client: %v", err)
	}

	start := time.Now()
	if err := client.Start(ctx); err != nil {
		log.Fatalf("Failed to start client: %v", err)
	}

	waitForCompletion(ctx, pool, int64(jobCount), 120*time.Second)
	elapsed := time.Since(start)

	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	client.Stop(stopCtx)

	jobsPerSec := float64(jobCount) / elapsed.Seconds()
	cleanJobs(ctx, pool)

	return BenchmarkResult{
		System:   "river",
		Scenario: "worker_throughput",
		Config: jsonRaw(map[string]interface{}{
			"job_count":    jobCount,
			"worker_count": workerCount,
		}),
		Results: jsonRaw(map[string]interface{}{
			"duration_ms":  elapsed.Milliseconds(),
			"jobs_per_sec": jobsPerSec,
		}),
	}
}

func scenarioPickupLatency(ctx context.Context, pool *pgxpool.Pool, iterations int, workerCount int) BenchmarkResult {
	cleanJobs(ctx, pool)

	workers := river.NewWorkers()
	river.AddWorker(workers, &BenchWorker{})

	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: workerCount},
		},
		Workers:           workers,
		JobTimeout:        -1,
		FetchCooldown:     50 * time.Millisecond,
		FetchPollInterval: 50 * time.Millisecond,
	})
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	if err := client.Start(ctx); err != nil {
		log.Fatalf("Failed to start client: %v", err)
	}
	time.Sleep(500 * time.Millisecond) // let client stabilise

	latenciesUs := make([]int64, 0, iterations)

	for i := 0; i < iterations; i++ {
		insertTime := time.Now()
		_, err := client.Insert(ctx, BenchArgs{Seq: int64(i)}, nil)
		if err != nil {
			log.Fatalf("Insert failed: %v", err)
		}
		waitForCompletion(ctx, pool, int64(i+1), 10*time.Second)
		latenciesUs = append(latenciesUs, time.Since(insertTime).Microseconds())
	}

	stopCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	client.Stop(stopCtx)

	sort.Slice(latenciesUs, func(i, j int) bool { return latenciesUs[i] < latenciesUs[j] })
	n := len(latenciesUs)
	p50 := latenciesUs[n/2]
	p95 := latenciesUs[int(float64(n)*0.95)]
	p99 := latenciesUs[int(float64(n)*0.99)]
	var sum int64
	for _, v := range latenciesUs {
		sum += v
	}
	mean := float64(sum) / float64(n)

	cleanJobs(ctx, pool)

	return BenchmarkResult{
		System:   "river",
		Scenario: "pickup_latency",
		Config: jsonRaw(map[string]interface{}{
			"iterations":   iterations,
			"worker_count": workerCount,
		}),
		Results: jsonRaw(map[string]interface{}{
			"mean_us": mean,
			"p50_us":  p50,
			"p95_us":  p95,
			"p99_us":  p99,
		}),
	}
}

// ── Long-horizon scenario ─────────────────────────────────────────

// LongHorizonArgs is the job payload for the long-horizon scenario. The padding
// field lets us approximate the declared payload size.
type LongHorizonArgs struct {
	Seq     int64  `json:"seq"`
	Padding string `json:"padding"`
}

func (LongHorizonArgs) Kind() string { return "long_horizon_job" }

type longHorizonState struct {
	mu           sync.Mutex
	latenciesMs  []latencyEvent
	completed    atomic.Uint64
	enqueued     atomic.Uint64
	queueDepth   atomic.Int64
	workMs       int
	maxLatencies int
}

type latencyEvent struct {
	recordedAt time.Time
	latencyMs  float64
}

type LongHorizonWorker struct {
	river.WorkerDefaults[LongHorizonArgs]
	state *longHorizonState
}

func (w *LongHorizonWorker) Work(ctx context.Context, job *river.Job[LongHorizonArgs]) error {
	// Pickup latency = now - created_at. River's Job struct exposes CreatedAt.
	latencyMs := float64(time.Since(job.CreatedAt).Milliseconds())
	if latencyMs < 0 {
		latencyMs = 0
	}
	w.state.mu.Lock()
	w.state.latenciesMs = append(w.state.latenciesMs, latencyEvent{time.Now(), latencyMs})
	if len(w.state.latenciesMs) > w.state.maxLatencies {
		// Drop oldest. Bounded memory is part of the contract.
		w.state.latenciesMs = w.state.latenciesMs[len(w.state.latenciesMs)-w.state.maxLatencies:]
	}
	w.state.mu.Unlock()
	if w.state.workMs > 0 {
		time.Sleep(time.Duration(w.state.workMs) * time.Millisecond)
	}
	w.state.completed.Add(1)
	return nil
}

// instanceID reads BENCH_INSTANCE_ID (0 if unset or malformed). Stamped
// onto every descriptor + sample record so the harness can attribute
// samples to the right replica without per-subprocess state.
func instanceID() int {
	raw := os.Getenv("BENCH_INSTANCE_ID")
	if raw == "" {
		return 0
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0
	}
	return n
}

// observerEnabled mirrors awa-bench's `observer_enabled`. Only instance 0
// emits cross-system observer metrics (queue depth, total backlog, producer
// target rate) so multi-replica runs report a single global observation
// instead of one per replica that the summary aggregator would have to
// de-duplicate later.
func observerEnabled() bool {
	return instanceID() == 0
}

// observerMetrics names the adapter metrics that describe a *global*
// observation (queue depth, total backlog) rather than this replica's
// per-instance behaviour. Only instance 0 emits these.
var observerMetrics = map[string]struct{}{
	"queue_depth":          {},
	"running_depth":        {},
	"retryable_depth":      {},
	"scheduled_depth":      {},
	"total_backlog":        {},
	"producer_target_rate": {},
}

func emitJSONL(rec map[string]interface{}) {
	if _, ok := rec["instance_id"]; !ok {
		rec["instance_id"] = instanceID()
	}
	b, err := json.Marshal(rec)
	if err != nil {
		return
	}
	fmt.Println(string(b))
}

func nowISO() string {
	return time.Now().UTC().Format("2006-01-02T15:04:05.000Z07:00")
}

func percentiles(state *longHorizonState, windowS float64) (p50, p95, p99 float64) {
	state.mu.Lock()
	cutoff := time.Now().Add(-time.Duration(windowS * float64(time.Second)))
	values := make([]float64, 0, len(state.latenciesMs))
	for _, e := range state.latenciesMs {
		if e.recordedAt.After(cutoff) {
			values = append(values, e.latencyMs)
		}
	}
	state.mu.Unlock()
	if len(values) == 0 {
		return 0, 0, 0
	}
	sort.Float64s(values)
	n := len(values)
	pick := func(q float64) float64 {
		idx := int(q * float64(n-1))
		if idx < 0 {
			idx = 0
		}
		if idx >= n {
			idx = n - 1
		}
		return values[idx]
	}
	return pick(0.50), pick(0.95), pick(0.99)
}

func runLongHorizon(ctx context.Context, pool *pgxpool.Pool, workerCount int) {
	producerRate := envInt("PRODUCER_RATE", 800)
	producerMode := envOrDefault("PRODUCER_MODE", "fixed")
	targetDepth := envInt("TARGET_DEPTH", 1000)
	sampleEveryS := envInt("SAMPLE_EVERY_S", 10)
	if sampleEveryS <= 0 {
		log.Fatalf("SAMPLE_EVERY_S must be > 0; got %d", sampleEveryS)
	}
	payloadBytes := envInt("JOB_PAYLOAD_BYTES", 256)
	workMs := envInt("JOB_WORK_MS", 1)

	dbName := "river_bench"
	if parsed, err := url.Parse(databaseURL()); err == nil {
		if name := strings.TrimPrefix(parsed.Path, "/"); name != "" {
			dbName = name
		}
	}

	emitJSONL(map[string]interface{}{
		"kind":           "descriptor",
		"system":         "river",
		"event_tables":   []string{"public.river_job"},
		"extensions":     []string{},
		"version":        "0.1.0",
		"schema_version": envOrDefault("RIVER_SCHEMA_VERSION", "current"),
		"db_name":        dbName,
		"started_at":     nowISO(),
	})

	state := &longHorizonState{
		workMs:       workMs,
		maxLatencies: 32768,
	}
	var producerTargetRate atomic.Int64
	producerTargetRate.Store(int64(producerRate))

	workers := river.NewWorkers()
	river.AddWorker(workers, &LongHorizonWorker{state: state})

	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Queues: map[string]river.QueueConfig{
			river.QueueDefault: {MaxWorkers: workerCount},
		},
		Workers:           workers,
		JobTimeout:        -1,
		FetchCooldown:     50 * time.Millisecond,
		FetchPollInterval: 50 * time.Millisecond,
	})
	if err != nil {
		log.Fatalf("long_horizon: failed to create client: %v", err)
	}
	if err := client.Start(ctx); err != nil {
		log.Fatalf("long_horizon: failed to start client: %v", err)
	}

	shutdown := make(chan struct{})
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	padding := strings.Repeat("x", maxInt(0, payloadBytes-32))

	// Producer
	var producerWG sync.WaitGroup
	producerWG.Add(1)
	go func() {
		defer producerWG.Done()
		var seq int64
		for {
			select {
			case <-shutdown:
				return
			default:
			}

			if producerMode == "depth-target" {
				producerTargetRate.Store(0)
				if state.queueDepth.Load() >= int64(targetDepth) {
					time.Sleep(50 * time.Millisecond)
					continue
				}
			} else {
				currentRate := readProducerRate(producerRate)
				producerTargetRate.Store(int64(currentRate))
				if currentRate <= 0 {
					time.Sleep(100 * time.Millisecond)
					continue
				}
				time.Sleep(time.Second / time.Duration(currentRate))
			}

			_, err := client.Insert(ctx, LongHorizonArgs{Seq: seq, Padding: padding}, nil)
			if err != nil {
				fmt.Fprintf(os.Stderr, "[river] producer insert failed: %v\n", err)
				continue
			}
			state.enqueued.Add(1)
			seq++
		}
	}()

	// Queue-depth poller
	var depthWG sync.WaitGroup
	depthWG.Add(1)
	go func() {
		defer depthWG.Done()
		if !observerEnabled() {
			// Non-zero replicas don't emit observer metrics; idle this
			// goroutine instead of polling. Saves N-1 connections of polling
			// work on a multi-replica run.
			<-shutdown
			return
		}
		tick := time.NewTicker(1 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-shutdown:
				return
			case <-tick.C:
				var n int64
				err := pool.QueryRow(ctx,
					"SELECT count(*) FROM river_job WHERE state = 'available'",
				).Scan(&n)
				if err == nil {
					state.queueDepth.Store(n)
				}
			}
		}
	}()

	// Sampler: clock-aligned to the sample_every_s boundary.
	var samplerWG sync.WaitGroup
	samplerWG.Add(1)
	go func() {
		defer samplerWG.Done()
		nowEpoch := time.Now().Unix()
		next := time.Unix(((nowEpoch/int64(sampleEveryS))+1)*int64(sampleEveryS), 0)
		// Sleepable wait: a SIGTERM during startup would otherwise block
		// here for up to SAMPLE_EVERY_S seconds. The select below makes
		// the initial alignment cancellable.
		alignTimer := time.NewTimer(time.Until(next))
		select {
		case <-shutdown:
			alignTimer.Stop()
			return
		case <-alignTimer.C:
		}
		lastEnq := state.enqueued.Load()
		lastCmp := state.completed.Load()
		lastTick := time.Now()
		ticker := time.NewTicker(time.Duration(sampleEveryS) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-shutdown:
				return
			case <-ticker.C:
				now := time.Now()
				dt := now.Sub(lastTick).Seconds()
				if dt < 0.001 {
					dt = 0.001
				}
				lastTick = now
				enq := state.enqueued.Load()
				cmp := state.completed.Load()
				enqRate := float64(enq-lastEnq) / dt
				cmpRate := float64(cmp-lastCmp) / dt
				lastEnq = enq
				lastCmp = cmp
				p50, p95, p99 := percentiles(state, 30.0)
				depth := float64(state.queueDepth.Load())
				targetRate := float64(producerTargetRate.Load())
				ts := nowISO()

				type metric struct {
					name    string
					value   float64
					windowS float64
				}
				for _, m := range []metric{
					{"claim_p50_ms", p50, 30},
					{"claim_p95_ms", p95, 30},
					{"claim_p99_ms", p99, 30},
					{"enqueue_rate", enqRate, float64(sampleEveryS)},
					{"completion_rate", cmpRate, float64(sampleEveryS)},
					{"queue_depth", depth, 0},
					{"producer_target_rate", targetRate, 0},
				} {
					if _, isObserver := observerMetrics[m.name]; isObserver && !observerEnabled() {
						continue
					}
					emitJSONL(map[string]interface{}{
						"t":            ts,
						"system":       "river",
						"kind":         "adapter",
						"subject_kind": "adapter",
						"subject":      "",
						"metric":       m.name,
						"value":        m.value,
						"window_s":     m.windowS,
					})
				}
			}
		}
	}()

	// Wait for SIGTERM / SIGINT.
	<-sigCh
	fmt.Fprintln(os.Stderr, "[river] long_horizon: shutdown signal received")
	close(shutdown)

	done := make(chan struct{})
	go func() {
		producerWG.Wait()
		depthWG.Wait()
		samplerWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		fmt.Fprintln(os.Stderr, "[river] long_horizon: timed out waiting for workers to stop")
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client.Stop(stopCtx)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ── Main ──────────────────────────────────────────────────────────

func main() {
	ctx := context.Background()
	pool := mustPool(ctx)
	defer pool.Close()

	migrate(ctx, pool)

	scenario := envOrDefault("SCENARIO", "all")
	jobCount := envInt("JOB_COUNT", 10000)
	workerCount := envInt("WORKER_COUNT", 50)
	latencyIterations := envInt("LATENCY_ITERATIONS", 100)

	var results []BenchmarkResult

	if scenario == "all" || scenario == "enqueue_throughput" {
		fmt.Fprintln(os.Stderr, "[river] Running enqueue_throughput...")
		results = append(results, scenarioEnqueueThroughput(ctx, pool, jobCount))
	}
	if scenario == "all" || scenario == "worker_throughput" {
		fmt.Fprintln(os.Stderr, "[river] Running worker_throughput...")
		results = append(results, scenarioWorkerThroughput(ctx, pool, jobCount, workerCount))
	}
	if scenario == "all" || scenario == "pickup_latency" {
		fmt.Fprintln(os.Stderr, "[river] Running pickup_latency...")
		results = append(results, scenarioPickupLatency(ctx, pool, latencyIterations, workerCount))
	}

	if scenario == "migrate_only" {
		fmt.Fprintln(os.Stderr, "[river] migrate_only: migrations applied, exiting.")
		return
	}

	if scenario == "long_horizon" {
		runLongHorizon(ctx, pool, workerCount)
		return
	}

	if scenario == "worker_only" {
		jobDurationMs := envInt("JOB_DURATION_MS", 30000)
		rescueAfterSecs := envInt("RESCUE_AFTER_SECS", 15)

		workers := river.NewWorkers()
		river.AddWorker(workers, &ChaosWorker{JobDurationMs: jobDurationMs})

		client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
			Queues: map[string]river.QueueConfig{
				river.QueueDefault: {MaxWorkers: workerCount},
			},
			Workers:              workers,
			RescueStuckJobsAfter: time.Duration(rescueAfterSecs) * time.Second,
			FetchCooldown:        50 * time.Millisecond,
			FetchPollInterval:    50 * time.Millisecond,
			JobTimeout:           -1,
		})
		if err != nil {
			log.Fatalf("Failed to create worker_only client: %v", err)
		}

		fmt.Fprintln(os.Stderr, "[river] worker_only: starting client...")
		if err := client.Start(ctx); err != nil {
			log.Fatalf("Failed to start worker_only client: %v", err)
		}

		fmt.Fprintln(os.Stderr, "[river] worker_only: client started, blocking forever (waiting for SIGKILL)...")
		select {}
	}

	out, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		log.Fatalf("Failed to marshal results: %v", err)
	}
	fmt.Println(string(out))
}
