import { MAX_REQUEST_EVENTS, type EventRow } from './events';

/** Leave room for Queues metadata inside its 64,000-byte billing unit. */
export const QUEUE_PACK_BYTES = 63_000;
export const CONSUMER_BATCH_EVENTS = 50;
type EventPack = { format: 'pvuv-events-v1'; events: EventRow[] };
export type QueuePayload = EventRow | EventPack;
const encoder = new TextEncoder();

/** Keep singleton rows in the legacy format; pack only within one request. */
export function packEvents(rows: EventRow[]): QueuePayload[] {
  const messages: QueuePayload[] = [];
  let pending: EventRow[] = [];
  const flush = (): void => {
    if (pending.length) messages.push(pending.length === 1 ? pending[0] : envelope(pending));
    pending = [];
  };
  for (const row of rows) {
    const candidate = [...pending, row];
    if (candidate.length > MAX_REQUEST_EVENTS
      || encoder.encode(JSON.stringify(envelope(candidate))).byteLength > QUEUE_PACK_BYTES) flush();
    pending.push(row);
    // An unusually large single event stays a legacy message (and may use
    // more than one billing unit), rather than gaining envelope overhead.
    if (encoder.encode(JSON.stringify(row)).byteLength > QUEUE_PACK_BYTES) flush();
  }
  flush();
  return messages;
}

function envelope(events: EventRow[]): EventPack {
  return { format: 'pvuv-events-v1', events };
}

/** Accept queued legacy rows during rollout; never silently discard a pack. */
export function unpackEvents(body: unknown): EventRow[] {
  if (isRow(body)) return [body];
  const pack = body as Partial<EventPack> | null;
  if (pack?.format === 'pvuv-events-v1' && Array.isArray(pack.events)
    && pack.events.length > 0 && pack.events.length <= MAX_REQUEST_EVENTS
    && pack.events.every(isRow)) return pack.events;
  throw new Error('Invalid PVUV queue payload');
}

function isRow(value: unknown): value is EventRow {
  const row = value as EventRow | null;
  return !!row && typeof row.site_id === 'string' && typeof row.event === 'string'
    && typeof row.visitor_id === 'string' && typeof row.session_id === 'string'
    && typeof row.ts === 'number';
}
