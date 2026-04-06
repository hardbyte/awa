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
	"os"
	"sort"
	"strconv"
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
