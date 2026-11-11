/**
 * pvuv.ai consumer worker — INGEST_QUEUE consumer (PROJECT_PLAN.md §18.3).
 *
 * Batch-writes enriched+scored EventRows into monthly events_YYYYMM tables
 * (created on demand from the shared template) and incrementally updates
 * sessions / identities / visitor_profiles.
 *
 * Exactly-once posture: event rows carry a stable `eid` and are written with
 * INSERT OR IGNORE against a UNIQUE index, so a Cloudflare Queues redelivery
 * (at-least-once) never duplicates an event row. All statements for one queue
 * batch go into a SINGLE atomic D1 batch() — no multi-chunk partial commits —
 * so a failure rolls the whole batch back and retryAll() re-runs it cleanly.
 * The dashboard's core metrics (pv/uv/verdict/page/country/device) are
 * recomputed from the exactly-once events by the rollup, so they are correct
 * under redelivery. The sessions/visitor_profiles counters are additive; in
 * the rare case an ack is lost AFTER a successful commit they could over-count
 * on redelivery — a bounded residual, full session-table exactly-once is M2.
 *
 * Write amplification: a queue batch (≤50 events) holds whole pages of one
 * visitor (the SDK flushes a page's pv/pulse/leave together), so the
 * session/identity/profile upserts are emitted ONE PER DISTINCT KEY PER
 * BATCH with folded counters — not one per event. The fold reproduces the
 * state the old per-event sequence would have left, including for a session
 * the batch itself creates (the INSERT side carries the folded totals). In
 * D1 each upserted row is a row written, so this is roughly −1.5 rows/event.
 *
 * Keep the queue's max_batch_size modest (see wrangler.toml) so one batch's
 * statements fit comfortably in a single D1 batch().
 */

import {
  monthSuffix, eventsTableDDL, ensureEventColumns, eventInsertSQL, eventRowValues, isConversion,
  type EventRow,
} from '../../../shared/events';
import { classifyChannel } from '../../../shared/channel';
import { SESSION_MAX_SPAN_MS } from '../../../shared/ids';

export interface Env {
  DB: D1Database;
}

/** Bounce definition (§9.3, GA4-style inverse): engaged = 2nd pageview OR
 *  dwell ≥ site threshold OR any custom event; bounce = not engaged. The dwell
 *  threshold is per-site (sites.engaged_seconds, default 15s). */
const DEFAULT_ENGAGED_MS = 15_000;

/** Months whose table+indexes this isolate has already ensured. */
const ensuredMonths = new Set<string>();

/** Per-site engagement dwell threshold in ms, cached per isolate (rarely changes). */
const siteEngagedMs = new Map<string, number>();

/** Load engagement thresholds for any site_ids not yet cached. */
async function ensureThresholds(db: D1Database, siteIds: string[]): Promise<void> {
  const missing = [...new Set(siteIds)].filter((id) => !siteEngagedMs.has(id));
  if (missing.length === 0) return;
  const rows = await db.prepare(
    `SELECT site_id, engaged_seconds FROM sites WHERE site_id IN (${missing.map(() => '?').join(',')})`,
  ).bind(...missing).all<{ site_id: string; engaged_seconds: number }>();
  const seen = new Set<string>();
  for (const r of rows.results) {
    siteEngagedMs.set(r.site_id, (r.engaged_seconds > 0 ? r.engaged_seconds : 15) * 1000);
    seen.add(r.site_id);
  }
  // sites with no row yet (e.g. KV-only) fall back to the default
  for (const id of missing) if (!seen.has(id)) siteEngagedMs.set(id, DEFAULT_ENGAGED_MS);
}

export default {
  async queue(batch: MessageBatch<unknown>, env: Env): Promise<void> {
    const rows = batch.messages.map((m) => m.body).filter(isPlausibleRow);
    if (rows.length === 0) {
      batch.ackAll();
      return;
    }

    try {
      // 1. ensure monthly partitions exist
      const months = [...new Set(rows.map((r) => monthSuffix(r.ts)))];
      for (const suffix of months) {
        if (ensuredMonths.has(suffix)) continue;
        await env.DB.batch(eventsTableDDL(suffix).map((sql) => env.DB.prepare(sql)));
        // A table that already existed is NOT recreated by the IF NOT EXISTS
        // above, so a month created before a later migration would still be
        // missing that migration's columns and every insert would fail. Repair
        // before writing — this runs once per isolate per month, so a deploy
        // self-heals on its first batch instead of waiting for the hourly cron.
        await ensureEventColumns(env.DB, suffix);
        ensuredMonths.add(suffix);
      }

      // per-site engagement dwell thresholds (for is_bounce)
      await ensureThresholds(env.DB, rows.map((r) => r.site_id));

      // 2. ONE atomic D1 batch: event inserts (OR IGNORE) + session/identity/
      //    profile upserts, ONE PER DISTINCT KEY (events of the same session
      //    cluster inside a queue batch). All-or-nothing, so a retry can't
      //    partial-commit.
      //
      //    ts-ordered once here: every fold below assumes the group is in
      //    application order, matching the per-event sequence it replaces.
      const ordered = [...rows].sort((a, b) => a.ts - b.ts);
      const stmts: D1PreparedStatement[] = [];
      for (const row of ordered) {
        stmts.push(env.DB.prepare(eventInsertSQL(monthSuffix(row.ts))).bind(...bindable(eventRowValues(row))));
      }
      const sessionGroups = groupBy(ordered, (r) => `${r.site_id}|${r.session_id}`);
      const visitorGroups = groupBy(ordered, (r) => `${r.site_id}|${r.visitor_id}`);
      const identityGroups = groupBy(
        ordered.filter((r) => r.user_id),
        (r) => `${r.site_id}|${r.user_id}|${r.visitor_id}`,
      );
      for (const evs of sessionGroups.values()) {
        stmts.push(sessionUpsert(env.DB, evs, siteEngagedMs.get(evs[0].site_id) ?? DEFAULT_ENGAGED_MS));
      }
      for (const evs of visitorGroups.values()) {
        stmts.push(profileUpsert(env.DB, evs));
      }
      for (const evs of identityGroups.values()) {
        stmts.push(identityUpsert(env.DB, evs));
      }
      await env.DB.batch(stmts);

      batch.ackAll();
    } catch (err) {
      console.error('consumer batch failed', err);
      batch.retryAll();
    }
  },
} satisfies ExportedHandler<Env>;

function isPlausibleRow(row: unknown): row is EventRow {
  const r = row as EventRow;
  return !!r && typeof r.site_id === 'string' && typeof r.event === 'string'
    && typeof r.visitor_id === 'string' && typeof r.session_id === 'string'
    && typeof r.ts === 'number';
}

/** D1 bind() rejects undefined — normalize to null. */
function bindable(values: unknown[]): unknown[] {
  return values.map((v) => (v === undefined ? null : v));
}

// ---------------------------------------------------------------------------
// Fold helpers — reduce a ts-ordered group to the parameters ONE upsert
// needs, equivalent to applying the old per-event statement sequence over
// the group in order.
// ---------------------------------------------------------------------------

function groupBy<T>(rows: T[], key: (row: T) => string): Map<string, T[]> {
  const groups = new Map<string, T[]>();
  for (const row of rows) {
    const k = key(row);
    const arr = groups.get(k);
    if (arr) arr.push(row); else groups.set(k, [row]);
  }
  return groups;
}

/** First row attaining the group's max bot_score. Ties keep the earliest —
 *  matching the strict > in the verdict CASE, which lets the existing verdict
 *  survive an equal-score later event. */
function worstRow(rows: EventRow[]): EventRow {
  return rows.reduce((worst, r) => (r.bot_score > worst.bot_score ? r : worst), rows[0]);
}

/** Last non-null value in application order — exactly what the per-event
 *  COALESCE(excluded.x, existing.x) chain applied in sequence leaves behind. */
function lastNonNull<T>(rows: EventRow[], pick: (r: EventRow) => T | null | undefined): T | null {
  let v: T | null = null;
  for (const r of rows) {
    const x = pick(r);
    if (x != null) v = x;
  }
  return v;
}

// ---------------------------------------------------------------------------
// sessions: incremental upsert (§3, §9.2), one per distinct (site, session)
// per queue batch. Attribution/entry fields come from the FIRST event (they
// are set on insert only); counters/exit fields are the fold of the group —
// so a session this batch itself creates lands in its final state in one
// statement instead of insert-then-N-updates.
// ---------------------------------------------------------------------------

function sessionUpsert(db: D1Database, rows: EventRow[], engagedMs: number): D1PreparedStatement {
  const first = rows[0];
  const last = rows[rows.length - 1];
  const pageviews = rows.reduce((n, r) => n + (r.event === 'pageview' ? 1 : 0), 0);
  let lastPv: EventRow | null = null;
  for (const r of rows) if (r.event === 'pageview') lastPv = r;
  // periodic behavior carriers accumulate dwell/flags but are NOT counted as
  // activity: events_count keeps meaning "things the visitor did"
  const counted = rows.reduce((n, r) => n + (r.event === 'page_pulse' ? 0 : 1), 0);
  const duration = rows.reduce((n, r) => n + (r.duration_ms ?? 0), 0);
  // a custom (goal) event is an engagement signal; identify/lifecycle events are not
  const anyConversion = rows.some((r) => isConversion(r.event)) ? 1 : 0;
  const hadInteraction = rows.reduce((m, r) => Math.max(m, r.had_interaction), 0);
  const worst = worstRow(rows);

  return db.prepare(`
    INSERT INTO sessions (
      session_id, site_id, visitor_id, user_id,
      entry_page, exit_page, entry_host, pageviews, events_count, duration_ms,
      had_interaction, had_pointer, is_bounce,
      source, medium, campaign, referrer, country, device_type,
      bot_score, verdict, bot_flags, started_at, last_active_at, last_pageview_at, channel
    ) VALUES (?1, ?2, ?3, ?4, ?5, ?27, ?21, ?6, ?25, ?7, ?8, ?26, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?28, ?23, ?24)
    ON CONFLICT(site_id, session_id) DO UPDATE SET
      user_id         = COALESCE(excluded.user_id, sessions.user_id),
      exit_page       = CASE WHEN ?6 >= 1 THEN excluded.exit_page ELSE sessions.exit_page END,
      -- Plausible/UA visit duration = last_pageview_at − started_at; only
      -- pageview events advance the last-pageview clock
      last_pageview_at = CASE WHEN ?6 >= 1 THEN MAX(COALESCE(sessions.last_pageview_at, 0), ?23) ELSE sessions.last_pageview_at END,
      pageviews       = sessions.pageviews + ?6,
      events_count    = sessions.events_count + ?25,
      -- clamped: a backgrounded tab keeps sending page_pulse, which kept
      -- extending last_active_at so the session never idled out. Production
      -- held sessions of 4.3 and 7.4 DAYS, and 18% of one site's sessions ran
      -- past 30 minutes, dragging the mean dwell to 35 min against a 4 min
      -- median-ish visit duration. A session cannot outlive its own span.
      duration_ms     = MIN(sessions.duration_ms + ?7, ${SESSION_MAX_SPAN_MS}),
      had_interaction = MAX(sessions.had_interaction, excluded.had_interaction),
      -- tri-state: stays NULL only while NEITHER side has ever reported, so an
      -- old loader's silence is never mistaken for "no pointer"
      had_pointer     = CASE
                          WHEN sessions.had_pointer IS NULL AND excluded.had_pointer IS NULL THEN NULL
                          ELSE MAX(COALESCE(sessions.had_pointer, 0), COALESCE(excluded.had_pointer, 0))
                        END,
      is_bounce       = CASE
                          WHEN sessions.pageviews + ?6 >= 2 THEN 0
                          WHEN sessions.duration_ms + ?7 >= ?22 THEN 0
                          WHEN ?20 = 1 THEN 0
                          ELSE sessions.is_bounce
                        END,
      bot_score       = MAX(sessions.bot_score, excluded.bot_score),
      bot_flags       = sessions.bot_flags | excluded.bot_flags,
      -- keep the verdict of the highest-scoring event; ties keep the existing
      -- verdict (strict >) so a later equal-score event can't flip e.g. a
      -- 'crawler' label (set independent of score) to 'clean'
      verdict         = CASE WHEN excluded.bot_score > sessions.bot_score
                             THEN excluded.verdict ELSE sessions.verdict END,
      last_active_at  = MAX(sessions.last_active_at, excluded.last_active_at)
  `).bind(
    first.session_id,                                     // 1
    first.site_id,                                        // 2
    first.visitor_id,                                     // 3
    lastNonNull(rows, (r) => r.user_id),                  // 4 user_id: last non-null
    first.path,                                           // 5 entry page (first event)
    pageviews,                                            // 6
    duration,                                             // 7
    hadInteraction,                                       // 8
    // folded initial bounce: the per-event sequence zeroes it once cumulative
    // pageviews ≥ 2, cumulative dwell ≥ threshold, or any custom event — a
    // session this batch creates must INSERT its final state directly
    pageviews >= 2 || anyConversion === 1 || duration >= engagedMs ? 0 : 1, // 9
    first.utm_source ?? first.ref_domain,                 // 10 source (utm first, else referrer domain)
    first.utm_medium,                                     // 11
    first.utm_campaign,                                   // 12
    first.referrer,                                       // 13
    first.country,                                        // 14
    first.device_type,                                    // 15
    worst.bot_score,                                      // 16
    worst.verdict,                                        // 17
    rows.reduce((f, r) => f | r.bot_flags, 0),            // 18
    first.ts,                                             // 19 started_at
    anyConversion,                                        // 20
    first.hostname,                                       // 21 entry_host (kept from first event)
    engagedMs,                                            // 22 per-site engaged dwell threshold
    lastPv ? lastPv.ts : null,                            // 23 last_pageview_at (pageview events only)
    // 24 channel — attribution is session-level, set on first insert only
    classifyChannel({
      refDomain: first.ref_domain, utmSource: first.utm_source,
      utmMedium: first.utm_medium, clickIdType: first.click_id_type,
    }),
    counted,                                              // 25 events_count increment
    // 26 tri-state pointer/keyboard: NULL only if no event ever reported it
    rows.every((r) => r.had_pointer == null) ? null : (rows.some((r) => r.had_pointer === 1) ? 1 : 0),
    lastPv ? lastPv.path : first.path,                    // 27 exit page (last pageview)
    last.ts,                                              // 28 last_active_at
  );
}

// ---------------------------------------------------------------------------
// identities: user ↔ visitor map, one upsert per distinct (site, user,
// visitor) per batch — only for events that carry a user_id (§3)
// ---------------------------------------------------------------------------

function identityUpsert(db: D1Database, rows: EventRow[]): D1PreparedStatement {
  return db.prepare(`
    INSERT INTO identities (site_id, user_id, visitor_id, traits, first_seen, last_seen)
    VALUES (?1, ?2, ?3, NULL, ?4, ?5)
    ON CONFLICT(site_id, user_id, visitor_id) DO UPDATE SET
      last_seen = MAX(identities.last_seen, excluded.last_seen)
  `).bind(rows[0].site_id, rows[0].user_id, rows[0].visitor_id, rows[0].ts, rows[rows.length - 1].ts);
}

// ---------------------------------------------------------------------------
// visitor_profiles: cheap incremental fields only, one per distinct visitor
// per batch. sessions_count and the Welford interval stats (interval_mean/
// m2/cv, active_hours) are maintained by the batch layer (§6.5, M2) — not
// double-counted here.
// ---------------------------------------------------------------------------

function profileUpsert(db: D1Database, rows: EventRow[]): D1PreparedStatement {
  const first = rows[0];
  const last = rows[rows.length - 1];
  const worst = worstRow(rows);
  // pulses keep the profile's last_seen fresh but don't inflate events_count
  const counted = rows.reduce((n, r) => n + (r.event === 'page_pulse' ? 0 : 1), 0);
  return db.prepare(`
    INSERT INTO visitor_profiles (
      site_id, visitor_id, events_count, sessions_count,
      fp_hash, ip24_hash, asn, bot_score, verdict, first_seen, last_seen
    ) VALUES (?1, ?2, ?9, 0, ?3, ?4, ?5, ?6, ?7, ?8, ?10)
    ON CONFLICT(site_id, visitor_id) DO UPDATE SET
      events_count = visitor_profiles.events_count + ?9,
      fp_hash      = COALESCE(excluded.fp_hash, visitor_profiles.fp_hash),
      ip24_hash    = COALESCE(excluded.ip24_hash, visitor_profiles.ip24_hash),
      asn          = COALESCE(excluded.asn, visitor_profiles.asn),
      bot_score    = MAX(visitor_profiles.bot_score, excluded.bot_score),
      verdict      = CASE WHEN excluded.bot_score > visitor_profiles.bot_score
                          THEN excluded.verdict ELSE visitor_profiles.verdict END,
      first_seen   = MIN(visitor_profiles.first_seen, excluded.first_seen),
      last_seen    = MAX(visitor_profiles.last_seen, excluded.last_seen)
  `).bind(
    first.site_id, first.visitor_id,
    lastNonNull(rows, (r) => r.fp_hash),
    lastNonNull(rows, (r) => r.ip24_hash),
    lastNonNull(rows, (r) => r.asn),
    worst.bot_score, worst.verdict,
    first.ts,
    counted,
    last.ts,
  );
}
