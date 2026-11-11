-- 0022: drop the two event-table indexes no hot query uses — idx_ev*_session
-- and idx_ev*_botcat. In D1 every index row counts as a row written, so each
-- of these cost one write on EVERY event insert (~22% of the per-event write
-- budget; 606MB of storage after 19 days). Precedent: 0020 dropped unused
-- visitor_profiles indexes for the same reason.
--
--   idx_ev*_session (session_id) — no query filters events by session_id:
--     journey timelines go through (site_id, visitor_id), and every
--     events↔sessions JOIN resolves on the sessions PK from the events side.
--     The one row-value-IN user (the daily batch's session-drift UPDATE)
--     falls back to scanning the day's window of the partition — once a day,
--     and rows read are three orders of magnitude cheaper than rows written.
--   idx_ev*_botcat (site_id, bot_category, ts) — only the crawler-category
--     dimension panel read it. That query already carries event='pageview'
--     and a ts window, so it rides idx_ev*_site_ev_ts like every other
--     dimension breakdown.
--
-- A migration can only name the partitions that exist when it is written:
-- the 0001 initial month plus this database's live months. The cron worker's
-- ensurePartitionSchema() drops the retired pair from every other partition
-- hourly (retiredIndexDropDDL in shared/events.ts), and new months are
-- created from the template without them.
DROP INDEX IF EXISTS idx_ev202607_session;
DROP INDEX IF EXISTS idx_ev202607_botcat;
DROP INDEX IF EXISTS idx_ev202610_session;
DROP INDEX IF EXISTS idx_ev202610_botcat;
DROP INDEX IF EXISTS idx_ev202611_session;
DROP INDEX IF EXISTS idx_ev202611_botcat;
