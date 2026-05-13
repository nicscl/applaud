/**
 * enrichment.ts
 *
 * Spawns the meeting-server-side enrichment pipeline
 * (`scripts/skills/process-latest-untranscribed.mjs`) as a detached child
 * process after a poll cycle that actually produced new audio or transcript
 * downloads. Single-instance locked — a tick that fires while a previous
 * enrichment is still running is silently dropped (the next post-poll tick
 * will pick the work up, because every downstream skill is idempotent).
 *
 * Configuration (env vars, evaluated lazily on each tick — no restart
 * needed when the user changes them):
 *
 *   APPLAUD_ENRICH_SCRIPT
 *     Absolute path to `process-latest-untranscribed.mjs`. Required for
 *     the hook to fire; if the path is unset or missing on disk, the
 *     enrichment chain is silently a no-op (applaud still works fine
 *     standalone — this is purely additive).
 *
 *   APPLAUD_ENRICHMENT_DISABLED=1
 *     Hard kill-switch. Returns `false` from `enrichmentEnabled()`
 *     regardless of script presence.
 *
 *   MEETING_SERVER_URL
 *     Forwarded to the child so the skill knows where to POST transcripts
 *     and read meeting metadata. Defaults to `http://127.0.0.1:47821`.
 *
 *   APPLAUD_RECORDINGS_DIR
 *     Forwarded to the child so it resolves audio paths the same way the
 *     poller does. The poller passes its own `cfg.recordingsDir` through
 *     when triggering, which takes precedence over any pre-existing env.
 */
import { spawn, type ChildProcess } from "node:child_process";
import { existsSync } from "node:fs";
import { logger } from "../logger.js";

const DEFAULT_MEETING_SERVER_URL = "http://127.0.0.1:47821";

let inFlight: ChildProcess | null = null;

export function enrichmentScript(): string | null {
  const p = process.env.APPLAUD_ENRICH_SCRIPT;
  if (!p) return null;
  if (!existsSync(p)) return null;
  return p;
}

export function enrichmentEnabled(): boolean {
  if (process.env.APPLAUD_ENRICHMENT_DISABLED === "1") return false;
  return enrichmentScript() !== null;
}

export function isEnrichmentRunning(): boolean {
  return inFlight !== null;
}

/**
 * Fire-and-forget. Returns true if a child was actually spawned, false if
 * skipped (disabled, missing script, or another run already in flight).
 *
 * `recordingsDir` is forwarded as `APPLAUD_RECORDINGS_DIR` so the underlying
 * skills resolve audio/transcript paths identically to the poller. Pass
 * null when called without a config (e.g. tests or pre-setup); the child
 * then falls back to its own hardcoded default.
 */
export function triggerEnrichment(opts: { recordingsDir: string | null }): boolean {
  const script = enrichmentScript();
  if (!script) return false;
  if (process.env.APPLAUD_ENRICHMENT_DISABLED === "1") return false;
  if (inFlight) {
    logger.info({ script }, "enrichment skipped: previous run still in flight");
    return false;
  }

  const env: NodeJS.ProcessEnv = {
    ...process.env,
    MEETING_SERVER_URL:
      process.env.MEETING_SERVER_URL || DEFAULT_MEETING_SERVER_URL,
  };
  if (opts.recordingsDir) {
    env.APPLAUD_RECORDINGS_DIR = opts.recordingsDir;
  }

  const startedAt = Date.now();
  const child = spawn(process.execPath, [script], {
    stdio: ["ignore", "pipe", "pipe"],
    env,
    detached: false,
  });
  inFlight = child;
  logger.info(
    { pid: child.pid, script, recordingsDir: opts.recordingsDir },
    "enrichment started",
  );

  // Bounded tails so a long noisy child doesn't grow memory unboundedly.
  // The child writes a single JSON summary as its last stdout line, which
  // is what we surface on close.
  let stdoutTail = "";
  let stderrTail = "";
  const cap = (s: string, add: string): string => {
    const next = s + add;
    return next.length > 8192 ? next.slice(-8192) : next;
  };
  child.stdout?.on("data", (d) => {
    stdoutTail = cap(stdoutTail, d.toString());
  });
  child.stderr?.on("data", (d) => {
    stderrTail = cap(stderrTail, d.toString());
  });
  child.on("error", (err) => {
    logger.warn({ err, script }, "enrichment spawn error");
    inFlight = null;
  });
  child.on("close", (code) => {
    const durationMs = Date.now() - startedAt;
    const lastLine = stdoutTail
      .split(/\r?\n/)
      .map((l) => l.trim())
      .filter(Boolean)
      .pop();
    let summary: unknown = null;
    if (lastLine) {
      try {
        summary = JSON.parse(lastLine);
      } catch {
        // Non-JSON tail; ignore — the summary stays null and the user can
        // still inspect _logs/process-latest.ndjson.
      }
    }
    if (code === 0) {
      logger.info(
        { exitCode: code, durationMs, summary },
        "enrichment finished",
      );
    } else {
      logger.warn(
        { exitCode: code, durationMs, stderrTail: stderrTail.slice(-500) },
        "enrichment exited non-zero",
      );
    }
    inFlight = null;
  });

  return true;
}
