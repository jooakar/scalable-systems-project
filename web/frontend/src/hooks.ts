import { useEffect, useRef, useState, useSyncExternalStore } from "react";

/** Health of the polling connection, shared by the status indicators. */
export interface Connection {
  lastSuccess: number | null;
  latencyMs: number | null;
  failing: boolean;
  error: string | null;
}

let connection: Connection = { lastSuccess: null, latencyMs: null, failing: false, error: null };
const listeners = new Set<() => void>();

function setConnection(next: Connection) {
  connection = next;
  listeners.forEach((listener) => listener());
}

export function useConnection(): Connection {
  return useSyncExternalStore(
    (listener) => {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    () => connection,
  );
}

export interface PollState<T> {
  data: T | undefined;
  error: string | null;
  loading: boolean;
}

interface Stored<T> {
  url: string | null;
  data: T | undefined;
  error: string | null;
}

/**
 * Polls a JSON endpoint. The next request starts only after the previous one
 * finishes, and the parsed value keeps its identity while the response body is
 * unchanged, so consumers do not redraw on every tick.
 */
export function usePoll<T>(
  url: string | null,
  intervalMs: number,
  options: { keepPrevious?: boolean } = {},
): PollState<T> {
  const [stored, setStored] = useState<Stored<T>>({ url: null, data: undefined, error: null });
  const lastBody = useRef<string | null>(null);

  useEffect(() => {
    if (url === null) {
      return undefined;
    }

    const controller = new AbortController();
    let timer: ReturnType<typeof setTimeout> | undefined;
    lastBody.current = null;

    async function load(target: string) {
      const started = performance.now();
      try {
        const response = await fetch(target, { signal: controller.signal });
        if (!response.ok) {
          throw new Error(`Request failed (${response.status})`);
        }
        const body = await response.text();
        if (controller.signal.aborted) {
          return;
        }
        setConnection({
          lastSuccess: Date.now(),
          latencyMs: performance.now() - started,
          failing: false,
          error: null,
        });
        if (body !== lastBody.current) {
          lastBody.current = body;
          setStored({ url: target, data: JSON.parse(body) as T, error: null });
        } else {
          setStored((current) => (current.error === null ? current : { ...current, error: null }));
        }
      } catch (cause) {
        if (controller.signal.aborted) {
          return;
        }
        const message = cause instanceof Error ? cause.message : "Request failed";
        setConnection({ ...connection, failing: true, error: message });
        setStored((current) => ({ ...current, error: message }));
      } finally {
        if (!controller.signal.aborted) {
          timer = setTimeout(() => load(target), intervalMs);
        }
      }
    }

    load(url);
    return () => {
      controller.abort();
      clearTimeout(timer);
    };
  }, [url, intervalMs]);

  const current = stored.url === url;
  return {
    data: current || options.keepPrevious ? stored.data : undefined,
    error: stored.error,
    loading: url !== null && !current,
  };
}

/** State persisted in localStorage; falls back to memory when storage is unavailable. */
export function useStoredState<T>(key: string, fallback: T): [T, (next: T | ((current: T) => T)) => void] {
  const [value, setValue] = useState<T>(() => {
    try {
      const raw = localStorage.getItem(key);
      return raw === null ? fallback : (JSON.parse(raw) as T);
    } catch {
      return fallback;
    }
  });

  useEffect(() => {
    try {
      localStorage.setItem(key, JSON.stringify(value));
    } catch {
      // Storage is a convenience only.
    }
  }, [key, value]);

  return [value, setValue];
}

/** Re-renders on an interval so relative times stay current. */
export function useNow(intervalMs: number): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = setInterval(() => setNow(Date.now()), intervalMs);
    return () => clearInterval(timer);
  }, [intervalMs]);
  return now;
}
