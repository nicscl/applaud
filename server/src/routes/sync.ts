import { Router } from "express";
import { poller } from "../sync/poller.js";
import { syncEvents } from "../sync/events.js";
import {
  countPendingTranscripts,
  countPendingSummaries,
  countErrorsLast24h,
  markWebhookFired,
} from "../sync/state.js";
import { getDb, rowToRecording, type RecordingDbRow } from "../db.js";
import { fireWebhookForRecording } from "../webhook/post.js";
import { loadConfig } from "../config.js";
import { logger } from "../logger.js";
import type { SyncStatusResponse } from "@applaud/shared";

export const syncRouter = Router();

syncRouter.get("/status", (_req, res) => {
  const s = poller.status();
  const resp: SyncStatusResponse = {
    lastPollAt: s.lastPollAt,
    nextPollAt: s.nextPollAt,
    polling: s.polling,
    pendingTranscripts: countPendingTranscripts(),
    pendingSummaries: countPendingSummaries(),
    errorsLast24h: countErrorsLast24h(),
    lastError: s.lastError,
    authRequired: s.authRequired,
  };
  res.json(resp);
});

syncRouter.post("/trigger", async (_req, res) => {
  await poller.trigger();
  res.json({ ok: true });
});

/**
 * One-shot backfill: re-fire any webhook that the poller would have fired
 * during normal asset download, but didn't (most common cause: webhook
 * URL was unconfigured at the time the asset landed). Walks the local
 * `recordings` table for rows where the asset is on disk but the
 * corresponding `webhook_*_fired_at` is still null, then calls
 * `fireWebhookForRecording` (same code path the poller uses).
 *
 * Safe to re-run: only touches rows whose webhook tracker is null, and
 * `markWebhookFired` is idempotent. Returns counts on success; an empty
 * webhook config short-circuits with `{ fired: 0, skipped: "no_webhook" }`.
 *
 * No body. POST-only because it mutates state (fires HTTP calls + updates
 * webhook timestamps).
 */
syncRouter.post("/refire-webhooks", async (_req, res) => {
  const cfg = loadConfig();
  if (!cfg.webhook?.url || !cfg.webhook.enabled) {
    res.json({ fired: 0, skipped: "no_webhook" });
    return;
  }

  const db = getDb();
  const audioRows = db
    .prepare<[], RecordingDbRow>(
      `SELECT * FROM recordings
       WHERE user_deleted_at IS NULL
         AND audio_downloaded_at IS NOT NULL
         AND webhook_audio_fired_at IS NULL
       ORDER BY start_time DESC`,
    )
    .all()
    .map(rowToRecording);
  const transcriptRows = db
    .prepare<[], RecordingDbRow>(
      `SELECT * FROM recordings
       WHERE user_deleted_at IS NULL
         AND transcript_downloaded_at IS NOT NULL
         AND webhook_transcript_fired_at IS NULL
       ORDER BY start_time DESC`,
    )
    .all()
    .map(rowToRecording);

  let firedAudio = 0;
  let firedTranscript = 0;
  let failedAudio = 0;
  let failedTranscript = 0;

  for (const row of audioRows) {
    try {
      const ok = await fireWebhookForRecording("audio_ready", row);
      if (ok) {
        markWebhookFired(row.id, "audio_ready");
        firedAudio++;
      } else {
        failedAudio++;
      }
    } catch (err) {
      logger.warn({ err, id: row.id }, "refire audio_ready failed");
      failedAudio++;
    }
  }

  for (const row of transcriptRows) {
    try {
      const ok = await fireWebhookForRecording("transcript_ready", row);
      if (ok) {
        markWebhookFired(row.id, "transcript_ready");
        firedTranscript++;
      } else {
        failedTranscript++;
      }
    } catch (err) {
      logger.warn({ err, id: row.id }, "refire transcript_ready failed");
      failedTranscript++;
    }
  }

  res.json({
    candidates: {
      audio: audioRows.length,
      transcript: transcriptRows.length,
    },
    fired: { audio: firedAudio, transcript: firedTranscript },
    failed: { audio: failedAudio, transcript: failedTranscript },
  });
});

syncRouter.get("/events", (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no");
  res.flushHeaders();

  const send = (payload: unknown): void => {
    res.write(`data: ${JSON.stringify(payload)}\n\n`);
  };
  send({ type: "subscribed" });

  const unsub = syncEvents.onEvent(send);
  const heartbeat = setInterval(() => {
    res.write(": heartbeat\n\n");
  }, 15_000);
  req.on("close", () => {
    unsub();
    clearInterval(heartbeat);
    res.end();
  });
});
