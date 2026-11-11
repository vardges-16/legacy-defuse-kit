//-----------------------------------------------------------------------
// Minimal async loader: loading / data / error, re-run when deps change.
// No retries: connector, permission and query errors surface immediately.
// Loading is derived (result key ≠ current key), so the effect only sets
// state from the async callback.
//-----------------------------------------------------------------------

import { useEffect, useState } from 'react';

export interface QueryState<T> {
  data: T | undefined;
  error: Error | null;
  loading: boolean;
}

type Result<T> = { key: string; data: T | undefined; error: Error | null };

/** `deps` must be primitives (ids, refresh counters): they form the cache key. */
export function useQuery<T>(fn: () => Promise<T>, deps: ReadonlyArray<string | number | boolean | null>): QueryState<T> {
  const key = JSON.stringify(deps);
  const [result, setResult] = useState<Result<T> | null>(null);

  useEffect(() => {
    let alive = true;
    fn().then(
      (data) => {
        if (alive) setResult({ key, data, error: null });
      },
      (e: unknown) => {
        if (alive) setResult({ key, data: undefined, error: e instanceof Error ? e : new Error(String(e)) });
      }
    );
    return () => {
      alive = false;
    };
    // `fn` is recreated every render; the primitive key decides when to reload.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [key]);

  const current = result !== null && result.key === key;
  return {
    data: current ? result.data : undefined,
    error: current ? result.error : null,
    loading: !current,
  };
}
