export class HttpError extends Error {
  constructor(message: string, public readonly status: number, public readonly retryAfterMs = 0) { super(message); }
}
export class ReadProtocolError extends Error {}
export class ReadTimeoutError extends Error {}

export function retryAfter(value: string | null, now = Date.now()): number {
  if (!value) return 1000;
  if (/^\d+$/.test(value)) {
    const ms = Number(value) * 1000;
    return Number.isSafeInteger(ms) ? ms : 60_000;
  }
  const time = Date.parse(value);
  return Number.isFinite(time) ? Math.max(0, time - now) : 1000;
}

let signal: AbortSignal | undefined;
let failure: ((error: unknown) => void) | undefined;
export function currentReadSignal() { return signal; }
export function withReadSignal<T>(value: AbortSignal, run: () => T): T {
  const previous = signal; signal = value;
  try { return run(); } finally { signal = previous; }
}
export function onHttpFailure(listener?: (error: unknown) => void) { failure = listener; }

export async function checkedJson<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const error = new HttpError(`Admin request failed (${response.status})`, response.status,
      [429, 503].includes(response.status) ? retryAfter(response.headers.get("retry-after")) : 0);
    failure?.(error); // Apply backend cooldown before waiting on an error body.
    if (response.headers.get("content-type")?.split(";")[0]?.trim() === "application/json") {
      try {
        const body: unknown = await response.json();
        if (body && typeof body === "object" && "error" in body && typeof body.error === "string") error.message = body.error;
      } catch (bodyError) {
        if (bodyError instanceof DOMException && bodyError.name === "AbortError") throw bodyError;
      }
    }
    throw error;
  }
  if (response.redirected || response.headers.get("content-type")?.split(";")[0]?.trim().toLowerCase() !== "application/json") {
    throw new ReadProtocolError("The server did not return the required JSON response");
  }
  try { return await response.json() as T; } catch (error) {
    if (error instanceof SyntaxError) throw new ReadProtocolError("Malformed JSON response");
    throw error;
  }
}

const deadlines = new WeakMap<AbortSignal, { expires: number; now: () => number; controller: AbortController }>();
export function assertReadCurrent(signal: AbortSignal) {
  const deadline = deadlines.get(signal);
  if (deadline && deadline.now() >= deadline.expires) {
    deadline.controller.abort();
    throw new ReadTimeoutError("Admin read exceeded its client deadline");
  }
  if (signal.aborted) throw new DOMException("Admin read cancelled", "AbortError");
}
/** Fence before a query library can publish into its own cache, including when
 * the event loop delays the timeout callback or the transport ignores abort. */
export async function readWithFence<T>(signal: AbortSignal, run: () => Promise<T>): Promise<T> {
  assertReadCurrent(signal);
  const value = await run();
  assertReadCurrent(signal);
  return value;
}

/** One timer for the complete fetch/body operation. Cancellation settles even
 * when a transport ignores AbortSignal; callers must fence late cache writes. */
export async function boundedRead<T>(controller: AbortController, timeoutMs: number, run: () => Promise<T>, now = () => performance.now()): Promise<T> {
  deadlines.set(controller.signal, { expires: now() + timeoutMs, now, controller });
  let timedOut = false;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let cancel = () => {};
  const cancellation = new Promise<never>((_resolve, reject) => {
    cancel = () => reject(new DOMException("Admin read cancelled", "AbortError"));
    controller.signal.addEventListener("abort", cancel, { once: true });
    timer = setTimeout(() => { timedOut = true; controller.abort(); }, timeoutMs);
  });
  try {
    const value = await Promise.race([cancellation, Promise.resolve().then(() => {
      if (controller.signal.aborted) throw new DOMException("Admin read cancelled", "AbortError");
      return run();
    })]);
    assertReadCurrent(controller.signal);
    return value;
  } catch (error) {
    if (timedOut || now() >= deadlines.get(controller.signal)!.expires) throw new ReadTimeoutError("Admin read exceeded its client deadline");
    throw error;
  } finally {
    if (timer !== undefined) clearTimeout(timer);
    controller.signal.removeEventListener("abort", cancel);
  }
}
