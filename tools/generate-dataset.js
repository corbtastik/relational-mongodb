#!/usr/bin/env node
'use strict';

/**
 * tools/generate-dataset.js
 *
 * Deterministic CarrierOps dataset generator.
 *
 * Outputs:
 *  - canonical (normalized NDJSON; single source of truth)
 *  - mongo_normalized (mirrors canonical; adds _id=<int> where applicable)
 *  - mongo_optimized (embeds subscriber profile + order items; optional featureCodes)
 *  - postgres CSVs (seed/postgres/data/*.csv) aligned to seed/postgres/tables.sql
 *  - manifest.json (seed/size/version/counts)
 *
 * Notes:
 *  - Integer IDs everywhere for join keys.
 *  - usage_records.units is generic:
 *      voice -> seconds, sms -> messages, data -> KB
 */

const fs = require('fs');
const path = require('path');

const DATASET_VERSION = '1.1.0';

// ---------- CLI ----------
function parseArgs(argv) {
  const args = {
    size: 'S',
    seed: 42,
    out: path.resolve(process.cwd(), 'seed'),
    only: 'all',          // canonical|mongo_normalized|mongo_optimized|postgres|all
    overwrite: false,
  };

  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--size') args.size = argv[++i];
    else if (a === '--seed') args.seed = Number(argv[++i]);
    else if (a === '--out') args.out = path.resolve(argv[++i]);
    else if (a === '--only') args.only = argv[++i];
    else if (a === '--overwrite') args.overwrite = true;
    else if (a === '-h' || a === '--help') {
      console.log(`Usage:
  node tools/generate-dataset.js --size S|M|L --seed 42 --out seed --only all --overwrite

--only values:
  canonical | mongo_normalized | mongo_optimized | postgres | all
`);
      process.exit(0);
    } else {
      throw new Error(`Unknown arg: ${a}`);
    }
  }

  if (!['S', 'M', 'L'].includes(args.size)) throw new Error(`Invalid --size ${args.size}`);
  if (!['canonical', 'mongo_normalized', 'mongo_optimized', 'postgres', 'all'].includes(args.only)) {
    throw new Error(`Invalid --only ${args.only}`);
  }
  if (!Number.isFinite(args.seed)) throw new Error(`Invalid --seed ${args.seed}`);

  return args;
}

// ---------- deterministic RNG ----------
function mulberry32(seed) {
  let t = seed >>> 0;
  return function rand() {
    t += 0x6D2B79F5;
    let r = Math.imul(t ^ (t >>> 15), 1 | t);
    r ^= r + Math.imul(r ^ (r >>> 7), 61 | r);
    return ((r ^ (r >>> 14)) >>> 0) / 4294967296;
  };
}

function choice(rng, arr) {
  return arr[Math.floor(rng() * arr.length)];
}

function intBetween(rng, min, maxInclusive) {
  const span = (maxInclusive - min + 1);
  return min + Math.floor(rng() * span);
}

function shuffle(rng, arr) {
  const a = arr.slice();
  for (let i = a.length - 1; i > 0; i--) {
    const j = Math.floor(rng() * (i + 1));
    [a[i], a[j]] = [a[j], a[i]];
  }
  return a;
}

function iso(dt) {
  return new Date(dt).toISOString().replace('.000Z', 'Z');
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function writeNdjson(filePath, rows) {
  const data = rows.map(r => JSON.stringify(r)).join('\n') + (rows.length ? '\n' : '');
  fs.writeFileSync(filePath, data, 'utf8');
}

function safeWrite(filePath, contents, overwrite) {
  if (!overwrite && fs.existsSync(filePath)) {
    throw new Error(`Refusing to overwrite existing file: ${filePath} (use --overwrite)`);
  }
  ensureDir(path.dirname(filePath));
  fs.writeFileSync(filePath, contents, 'utf8');
}

// ---------- CSV helpers ----------
function csvEscape(val) {
  // Postgres COPY CSV: quote if contains comma/quote/newline; quotes doubled.
  // Nulls: empty field.
  if (val === null || val === undefined) return '';
  let s = String(val);

  // normalize newlines
  s = s.replace(/\r\n/g, '\n').replace(/\r/g, '\n');

  const needsQuotes = s.includes(',') || s.includes('"') || s.includes('\n');
  if (s.includes('"')) s = s.replace(/"/g, '""');
  return needsQuotes ? `"${s}"` : s;
}

function toCsv(headers, rows) {
  const lines = [];
  lines.push(headers.join(','));
  for (const r of rows) {
    const line = headers.map(h => csvEscape(r[h])).join(',');
    lines.push(line);
  }
  return lines.join('\n') + '\n';
}

function jsonString(v) {
  if (v === null || v === undefined) return null;
  return JSON.stringify(v);
}

// ---------- presets ----------
function presets(size) {
  if (size === 'S') {
    return {
      accounts: 3,
      subsPerAccountMin: 2, subsPerAccountMax: 2,
      ordersPerAccountMin: 1, ordersPerAccountMax: 2,
      itemsPerOrderMin: 2, itemsPerOrderMax: 3,
      ticketsPerSubscriberMin: 0, ticketsPerSubscriberMax: 1,
      deviceEventsPerDeviceMin: 3, deviceEventsPerDeviceMax: 6,
      usageRecordsPerSubscriber: 10,
      days: 2,
      embedFeatureCodes: true,
    };
  }
  if (size === 'M') {
    return {
      accounts: 25,
      subsPerAccountMin: 2, subsPerAccountMax: 5,
      ordersPerAccountMin: 1, ordersPerAccountMax: 4,
      itemsPerOrderMin: 2, itemsPerOrderMax: 5,
      ticketsPerSubscriberMin: 0, ticketsPerSubscriberMax: 2,
      deviceEventsPerDeviceMin: 20, deviceEventsPerDeviceMax: 60,
      usageRecordsPerSubscriber: 250,
      days: 7,
      embedFeatureCodes: true,
    };
  }
  // L
  return {
    accounts: 200,
    subsPerAccountMin: 2, subsPerAccountMax: 6,
    ordersPerAccountMin: 1, ordersPerAccountMax: 5,
    itemsPerOrderMin: 2, itemsPerOrderMax: 6,
    ticketsPerSubscriberMin: 0, ticketsPerSubscriberMax: 2,
    deviceEventsPerDeviceMin: 200, deviceEventsPerDeviceMax: 600,
    usageRecordsPerSubscriber: 2500,
    days: 30,
    embedFeatureCodes: true,
  };
}

// ---------- vocab ----------
const REGION_CODES = ['TX-NORTH', 'TX-SOUTH'];
const ACCOUNT_NAMES = [
  'Oak Hill Coffee Co',
  'Red River Hardware',
  'Pecan Street Apartments',
  'Bluebonnet Clinic',
  'Cedar Ridge Auto',
  'Trinity Bookshop',
];

const DEVICE_MODELS = ['Pixel 8', 'Pixel 7a', 'iPhone 15', 'iPhone 14', 'Galaxy S24', 'Galaxy A54'];

const ORDER_SKUS = [
  { sku: 'SIM-ESIM', priceCents: 0 },
  { sku: 'SIM-PHYSICAL', priceCents: 500 },
  { sku: 'PLAN-UNL-5G', priceCents: 6500 },
  { sku: 'PLAN-5G-STARTER', priceCents: 4500 },
  { sku: 'ADDON-HOTSPOT', priceCents: 1000 },
  { sku: 'ADDON-INTL_ROAM', priceCents: 1500 },
  { sku: 'ADDON-DEVICE_PROTECT', priceCents: 1700 },
  { sku: 'DEVICE-PHONE', priceCents: 79900 },
];

const FEATURE_DEFS = [
  { featureId: 5001, code: 'HOTSPOT', name: 'Mobile Hotspot' },
  { featureId: 5002, code: 'INTL_ROAM', name: 'International Roaming Pack' },
  { featureId: 5003, code: 'DEVICE_PROTECT', name: 'Device Protection' },
  { featureId: 5004, code: 'VISUAL_VM', name: 'Visual Voicemail' },
];

const TICKET_STATUS = [
  { code: 'OPEN', description: 'Open / Investigating' },
  { code: 'WIP', description: 'Work in Progress' },
  { code: 'RESOLVED', description: 'Resolved' },
];

const TICKET_SUMMARIES = [
  'Intermittent data connectivity in downtown area',
  'Unable to activate device after SIM swap',
  'Voicemail not syncing',
  'Roaming add-on stuck in provisioning',
  'High latency during evening hours',
  'Dropped calls reported near highway corridor',
];

const NOTE_AUTHORS = ['opsAgent7', 'netOps2', 'care1', 'care2', 'fulfill3', 'netOps4'];

// ---------- IDs ----------
function idRanges() {
  return {
    accountId: 1001,
    subscriberId: 2001,
    deviceId: 3001,
    orderId: 4001,
    orderItemId: 4101,
    subscriberFeatureStateId: 6001,
    ticketId: 7001,
    noteId: 8001,
    orgUnitId: 9001,
    planId: 10001,
    regionId: 11001,
    deviceClassId: 12001,
    deviceEventId: 13001,
    usageRecordId: 14001,
  };
}

function padNum(n, width) {
  return String(n).padStart(width, '0');
}

// ---------- generation ----------
function generateCanonical(rng, cfg) {
  const ids = idRanges();

  const plans = [
    { planId: ids.planId++, code: 'PLAN-UNL-5G', name: 'Unlimited 5G' },
    { planId: ids.planId++, code: 'PLAN-5G-STARTER', name: '5G Starter' },
  ];

  const regions = REGION_CODES.map((code) => ({ regionId: ids.regionId++, code }));
  const deviceClasses = [
    { deviceClassId: ids.deviceClassId++, code: 'PHONE' },
    { deviceClassId: ids.deviceClassId++, code: 'TABLET' },
  ];

  const orgUnits = [
    { orgUnitId: 9001, name: 'Network Ops', parentOrgUnitId: null },
    { orgUnitId: 9002, name: 'Core Network', parentOrgUnitId: 9001 },
    { orgUnitId: 9003, name: 'Radio Access Network', parentOrgUnitId: 9001 },
    { orgUnitId: 9004, name: 'Customer Care', parentOrgUnitId: null },
    { orgUnitId: 9005, name: 'Field Ops', parentOrgUnitId: 9004 },
  ];

  const rates = [];
  for (const p of plans) {
    for (const r of regions) {
      for (const dc of deviceClasses) {
        const base = (p.code === 'PLAN-UNL-5G') ? 6500 : 4500;
        const regionDelta = (r.code === 'TX-NORTH') ? 0 : -200;
        const classDelta = (dc.code === 'PHONE') ? 0 : -1000;
        rates.push({
          planId: p.planId,
          regionId: r.regionId,
          deviceClassId: dc.deviceClassId,
          rateCents: base + regionDelta + classDelta
        });
      }
    }
  }

  const base = new Date('2026-01-18T00:00:00Z').getTime();
  const dayMs = 24 * 60 * 60 * 1000;

  // Accounts
  const accounts = [];
  for (let i = 0; i < cfg.accounts; i++) {
    const accountId = ids.accountId++;
    const name = ACCOUNT_NAMES[i % ACCOUNT_NAMES.length];
    const billingRegionCode = choice(rng, REGION_CODES);
    const createdAt = new Date(base - (cfg.days + 5) * dayMs + i * 2 * 60 * 60 * 1000);

    accounts.push({
      accountId,
      accountNumber: `A-${accountId}`,
      name,
      billingRegionCode,
      status: 'active',
      createdAt: iso(createdAt),
    });
  }

  // Subscribers + profiles
  const subscribers = [];
  const subscriberProfiles = [];

  const firstNames = ['Alex', 'Jordan', 'Casey', 'Taylor', 'Morgan', 'Riley', 'Sam', 'Drew'];
  const lastNames = ['Bennett', 'Kim', 'Nguyen', 'Reed', 'Patel', 'Santos', 'Carter', 'Lopez'];

  for (const acct of accounts) {
    const subsCount = intBetween(rng, cfg.subsPerAccountMin, cfg.subsPerAccountMax);
    for (let j = 0; j < subsCount; j++) {
      const subscriberId = ids.subscriberId++;
      const msisdn = `+1214555${padNum(subscriberId % 10000, 4)}`;
      const createdAt = new Date(base - (cfg.days + 3) * dayMs + (subscriberId % 20) * 15 * 60 * 1000);

      subscribers.push({
        subscriberId,
        accountId: acct.accountId,
        msisdn,
        status: (rng() < 0.1) ? 'suspended' : 'active',
        createdAt: iso(createdAt),
      });

      const fn = choice(rng, firstNames);
      const ln = choice(rng, lastNames);
      const dobYear = intBetween(rng, 1980, 1998);
      const dobMonth = intBetween(rng, 1, 12);
      const dobDay = intBetween(rng, 1, 28);
      const updatedAt = new Date(base - (cfg.days) * dayMs + intBetween(rng, 1, 12) * 60 * 60 * 1000);

      subscriberProfiles.push({
        subscriberId,
        firstName: fn,
        lastName: ln,
        email: `${fn.toLowerCase()}.${ln.toLowerCase()}@example.com`,
        dob: `${dobYear}-${padNum(dobMonth, 2)}-${padNum(dobDay, 2)}`,
        piiLast4Ssn: padNum(intBetween(rng, 0, 9999), 4),
        preferences: {
          marketingOptIn: rng() < 0.5,
          paperlessBilling: rng() < 0.7
        },
        updatedAt: iso(updatedAt),
      });
    }
  }

  // Devices (1 per subscriber)
  const devices = [];
  for (const s of subscribers) {
    const deviceId = ids.deviceId++;
    const imei = `356789012${padNum(deviceId, 6)}`;
    devices.push({
      deviceId,
      subscriberId: s.subscriberId,
      imei,
      model: choice(rng, DEVICE_MODELS),
      createdAt: iso(new Date(new Date(s.createdAt).getTime() + 30 * 60 * 1000)),
    });
  }

  // Orders + items
  const orders = [];
  const orderItems = [];
  for (const acct of accounts) {
    const oCount = intBetween(rng, cfg.ordersPerAccountMin, cfg.ordersPerAccountMax);
    for (let k = 0; k < oCount; k++) {
      const orderId = ids.orderId++;
      const statuses = ['submitted', 'fulfilled', 'canceled'];
      const status = choice(rng, statuses);
      const createdAt = new Date(base - (cfg.days) * dayMs + intBetween(rng, 1, 36) * 60 * 60 * 1000);

      orders.push({
        orderId,
        accountId: acct.accountId,
        status,
        createdAt: iso(createdAt),
      });

      const itemsCount = intBetween(rng, cfg.itemsPerOrderMin, cfg.itemsPerOrderMax);
      const skus = shuffle(rng, ORDER_SKUS).slice(0, itemsCount);
      for (const s of skus) {
        orderItems.push({
          orderItemId: ids.orderItemId++,
          orderId,
          sku: s.sku,
          qty: 1,
          priceCents: s.priceCents,
        });
      }
    }
  }

  // Features + subscriber_features (M:N)
  const features = FEATURE_DEFS.map(x => ({ ...x }));
  const subscriberFeatures = [];

  for (const s of subscribers) {
    const count = intBetween(rng, 1, 2);
    const chosen = shuffle(rng, features).slice(0, count);
    for (const f of chosen) {
      subscriberFeatures.push({ subscriberId: s.subscriberId, featureId: f.featureId });
    }
  }

  // subscriber_feature_state (associative w/ attributes)
  const subscriberFeatureState = [];
  const sources = ['self-serve', 'call-center', 'system'];
  const states = ['pending', 'active', 'failed'];

  for (const sf of subscriberFeatures) {
    if (rng() < 0.55) continue;

    const effectiveFrom = new Date(base - (cfg.days + 1) * dayMs + intBetween(rng, 1, 48) * 60 * 60 * 1000);
    const provisioningState = (rng() < 0.75) ? 'active' : choice(rng, states);
    const source = choice(rng, sources);
    const effectiveTo = (rng() < 0.15) ? iso(new Date(effectiveFrom.getTime() + 3 * dayMs)) : null;

    subscriberFeatureState.push({
      subscriberFeatureStateId: ids.subscriberFeatureStateId++,
      subscriberId: sf.subscriberId,
      featureId: sf.featureId,
      effectiveFrom: iso(effectiveFrom),
      effectiveTo,
      provisioningState,
      source,
    });
  }

  // Ticket status codes + tickets
  const ticketStatusCodes = TICKET_STATUS.map(x => ({ ...x }));
  const tickets = [];
  for (const s of subscribers) {
    const tCount = intBetween(rng, cfg.ticketsPerSubscriberMin, cfg.ticketsPerSubscriberMax);
    for (let i = 0; i < tCount; i++) {
      const ticketId = ids.ticketId++;
      const statusCode = choice(rng, ticketStatusCodes).code;
      const openedAt = new Date(base - (cfg.days) * dayMs + intBetween(rng, 1, 48) * 60 * 60 * 1000);
      const isResolved = statusCode === 'RESOLVED';
      const closedAt = isResolved ? iso(new Date(openedAt.getTime() + intBetween(rng, 15, 180) * 60 * 1000)) : null;

      tickets.push({
        ticketId,
        subscriberId: s.subscriberId,
        statusCode,
        openedAt: iso(openedAt),
        closedAt,
        summary: choice(rng, TICKET_SUMMARIES),
      });
    }
  }

  // Notes (polymorphic)
  const notes = [];
  const someSubscribers = shuffle(rng, subscribers).slice(0, Math.min(4, subscribers.length));
  const someOrders = shuffle(rng, orders).slice(0, Math.min(3, orders.length));
  const someTickets = shuffle(rng, tickets).slice(0, Math.min(4, tickets.length));

  const noteRefs = [];
  for (const s of someSubscribers) noteRefs.push({ refType: 'subscriber', refId: s.subscriberId });
  for (const o of someOrders) noteRefs.push({ refType: 'order', refId: o.orderId });
  for (const t of someTickets) noteRefs.push({ refType: 'ticket', refId: t.ticketId });

  for (const r of noteRefs) {
    const createdAt = new Date(base - (cfg.days) * dayMs + intBetween(rng, 1, 48) * 60 * 60 * 1000);
    notes.push({
      noteId: ids.noteId++,
      refType: r.refType,
      refId: r.refId,
      author: choice(rng, NOTE_AUTHORS),
      body: `Note on ${r.refType} ${r.refId}: ${choice(rng, [
        'Investigating.',
        'Collecting logs.',
        'Escalated to ops.',
        'Customer contacted.',
        'Retrying provisioning.',
        'Monitoring impact.'
      ])}`,
      createdAt: iso(createdAt),
    });
  }

  // Device events
  const deviceEvents = [];
  const eventTypes = ['radio_attach', 'handover', 'data_session_start', 'data_session_end', 'latency_probe', 'attach_fail'];
  for (const d of devices) {
    const eCount = intBetween(rng, cfg.deviceEventsPerDeviceMin, cfg.deviceEventsPerDeviceMax);
    for (let i = 0; i < eCount; i++) {
      const ts = new Date(base - cfg.days * dayMs + intBetween(rng, 1, cfg.days * 24 * 60) * 60 * 1000);
      const eventType = choice(rng, eventTypes);
      const payload = (() => {
        switch (eventType) {
          case 'radio_attach': return { cellId: `DFW-${intBetween(rng, 100, 399)}`, rssi: -intBetween(rng, 70, 100) };
          case 'handover': return { fromCellId: `DFW-${intBetween(rng, 100, 399)}`, toCellId: `DFW-${intBetween(rng, 100, 399)}` };
          case 'data_session_start': return { apn: 'carrierops', ip: `10.${intBetween(rng, 0, 255)}.${intBetween(rng, 0, 255)}.${intBetween(rng, 1, 254)}` };
          case 'data_session_end': return { bytesUp: intBetween(rng, 1_000_000, 8_000_000), bytesDown: intBetween(rng, 3_000_000, 30_000_000) };
          case 'latency_probe': return { p50Ms: intBetween(rng, 20, 80), p95Ms: intBetween(rng, 100, 350) };
          case 'attach_fail': return { cause: choice(rng, ['network_congestion', 'auth_reject', 'radio_no_service']) };
          default: return {};
        }
      })();

      deviceEvents.push({
        deviceEventId: ids.deviceEventId++,
        deviceId: d.deviceId,
        eventType,
        ts: iso(ts),
        payload,
      });
    }
  }

  // Usage records
  const usageRecords = [];
  const usageTypes = ['voice', 'sms', 'data'];
  for (const s of subscribers) {
    for (let i = 0; i < cfg.usageRecordsPerSubscriber; i++) {
      const ts = new Date(base - cfg.days * dayMs + intBetween(rng, 1, cfg.days * 24 * 60) * 60 * 1000);
      const usageType = choice(rng, usageTypes);
      const units = (() => {
        if (usageType === 'voice') return intBetween(rng, 30, 600); // seconds
        if (usageType === 'sms') return intBetween(rng, 1, 5);     // messages
        return intBetween(rng, 256, 10240);                        // KB
      })();
      const ratedCents = (() => {
        if (usageType === 'sms') return 0;
        if (usageType === 'voice') return Math.ceil(units / 60) * 4;
        return Math.ceil(units / 1024) * 2;
      })();

      usageRecords.push({
        usageRecordId: ids.usageRecordId++,
        subscriberId: s.subscriberId,
        ts: iso(ts),
        usageType,
        units,
        ratedCents,
      });
    }
  }

  // Sort for readability
  accounts.sort((a, b) => a.accountId - b.accountId);
  subscribers.sort((a, b) => a.subscriberId - b.subscriberId);
  subscriberProfiles.sort((a, b) => a.subscriberId - b.subscriberId);
  devices.sort((a, b) => a.deviceId - b.deviceId);
  orders.sort((a, b) => a.orderId - b.orderId);
  orderItems.sort((a, b) => a.orderItemId - b.orderItemId);
  subscriberFeatures.sort((a, b) => (a.subscriberId - b.subscriberId) || (a.featureId - b.featureId));
  subscriberFeatureState.sort((a, b) => a.subscriberFeatureStateId - b.subscriberFeatureStateId);
  tickets.sort((a, b) => a.ticketId - b.ticketId);
  notes.sort((a, b) => a.noteId - b.noteId);
  deviceEvents.sort((a, b) => a.deviceEventId - b.deviceEventId);
  usageRecords.sort((a, b) => a.usageRecordId - b.usageRecordId);

  return {
    accounts,
    subscribers,
    subscriber_profiles: subscriberProfiles,
    devices,
    device_events: deviceEvents,
    orders,
    order_items: orderItems,
    features,
    subscriber_features: subscriberFeatures,
    subscriber_feature_state: subscriberFeatureState,
    ticket_status_codes: ticketStatusCodes,
    tickets,
    notes,
    org_units: orgUnits,
    plans,
    regions,
    device_classes: deviceClasses,
    rates,
    usage_records: usageRecords,
  };
}

// ---------- projections ----------
function toMongoNormalized(canonical) {
  const out = {};
  let seq = 1;

  const withId = (rows, idField) => rows.map(r => ({ _id: r[idField], ...r }));

  out.accounts = withId(canonical.accounts, 'accountId');
  out.subscribers = withId(canonical.subscribers, 'subscriberId');
  out.subscriber_profiles = withId(canonical.subscriber_profiles, 'subscriberId');
  out.devices = withId(canonical.devices, 'deviceId');
  out.device_events = withId(canonical.device_events, 'deviceEventId');
  out.orders = withId(canonical.orders, 'orderId');
  out.order_items = withId(canonical.order_items, 'orderItemId');
  out.features = withId(canonical.features, 'featureId');

  // join tables: deterministic int _id
  out.subscriber_features = canonical.subscriber_features.map(r => ({ _id: seq++, ...r }));
  out.subscriber_feature_state = withId(canonical.subscriber_feature_state, 'subscriberFeatureStateId');

  // lookup: _id as code (string) is most natural
  out.ticket_status_codes = canonical.ticket_status_codes.map(r => ({ _id: r.code, ...r }));

  out.tickets = withId(canonical.tickets, 'ticketId');
  out.notes = withId(canonical.notes, 'noteId');
  out.org_units = withId(canonical.org_units, 'orgUnitId');

  out.plans = withId(canonical.plans, 'planId');
  out.regions = withId(canonical.regions, 'regionId');
  out.device_classes = withId(canonical.device_classes, 'deviceClassId');

  // rates: composite key in relational; deterministic int _id
  out.rates = canonical.rates.map(r => ({ _id: seq++, ...r }));

  out.usage_records = withId(canonical.usage_records, 'usageRecordId');

  return out;
}

function toMongoOptimized(canonical, mongoNormalized, cfg) {
  const out = { ...mongoNormalized };

  const profileBySubId = new Map(
    canonical.subscriber_profiles.map(p => [p.subscriberId, p])
  );

  const featureById = new Map(canonical.features.map(f => [f.featureId, f]));
  const featureCodesBySub = new Map();
  for (const sf of canonical.subscriber_features) {
    const f = featureById.get(sf.featureId);
    if (!f) continue;
    if (!featureCodesBySub.has(sf.subscriberId)) featureCodesBySub.set(sf.subscriberId, []);
    featureCodesBySub.get(sf.subscriberId).push(f.code);
  }

  out.subscribers = mongoNormalized.subscribers.map(s => {
    const base = { ...s };
    const prof = profileBySubId.get(s.subscriberId);
    if (prof) {
      const { subscriberId, ...rest } = prof;
      base.profile = rest;
    }
    if (cfg.embedFeatureCodes) {
      const codes = featureCodesBySub.get(s.subscriberId) || [];
      base.featureCodes = Array.from(new Set(codes)).sort();
    }
    return base;
  });

  const itemsByOrderId = new Map();
  for (const it of mongoNormalized.order_items) {
    if (!itemsByOrderId.has(it.orderId)) itemsByOrderId.set(it.orderId, []);
    itemsByOrderId.get(it.orderId).push({
      orderItemId: it.orderItemId,
      sku: it.sku,
      qty: it.qty,
      priceCents: it.priceCents
    });
  }

  out.orders = mongoNormalized.orders.map(o => ({
    ...o,
    items: (itemsByOrderId.get(o.orderId) || []).sort((a, b) => a.orderItemId - b.orderItemId)
  }));

  // Clean optimized view: embedded, so drop these collections
  delete out.order_items;
  delete out.subscriber_profiles;

  return out;
}

// ---------- Postgres CSV projection (snake_case headers aligned to DDL) ----------
function toPostgresCsvBundles(canonical) {
  // Each entry: { filename, headers, rows }
  const bundles = [];

  bundles.push({
    filename: 'accounts.csv',
    headers: ['account_id', 'account_number', 'name', 'billing_region_code', 'status', 'created_at'],
    rows: canonical.accounts.map(r => ({
      account_id: r.accountId,
      account_number: r.accountNumber,
      name: r.name,
      billing_region_code: r.billingRegionCode,
      status: r.status,
      created_at: r.createdAt,
    })),
  });

  bundles.push({
    filename: 'subscribers.csv',
    headers: ['subscriber_id', 'account_id', 'msisdn', 'status', 'created_at'],
    rows: canonical.subscribers.map(r => ({
      subscriber_id: r.subscriberId,
      account_id: r.accountId,
      msisdn: r.msisdn,
      status: r.status,
      created_at: r.createdAt,
    })),
  });

  bundles.push({
    filename: 'subscriber_profiles.csv',
    headers: ['subscriber_id', 'first_name', 'last_name', 'email', 'dob', 'pii_last4_ssn', 'preferences', 'updated_at'],
    rows: canonical.subscriber_profiles.map(r => ({
      subscriber_id: r.subscriberId,
      first_name: r.firstName ?? null,
      last_name: r.lastName ?? null,
      email: r.email ?? null,
      dob: r.dob ?? null,
      pii_last4_ssn: r.piiLast4Ssn ?? null,
      preferences: jsonString(r.preferences),
      updated_at: r.updatedAt,
    })),
  });

  bundles.push({
    filename: 'devices.csv',
    headers: ['device_id', 'subscriber_id', 'imei', 'model', 'created_at'],
    rows: canonical.devices.map(r => ({
      device_id: r.deviceId,
      subscriber_id: r.subscriberId,
      imei: r.imei,
      model: r.model,
      created_at: r.createdAt,
    })),
  });

  bundles.push({
    filename: 'device_events.csv',
    headers: ['device_event_id', 'device_id', 'event_type', 'ts', 'payload'],
    rows: canonical.device_events.map(r => ({
      device_event_id: r.deviceEventId,
      device_id: r.deviceId,
      event_type: r.eventType,
      ts: r.ts,
      payload: jsonString(r.payload),
    })),
  });

  bundles.push({
    filename: 'orders.csv',
    headers: ['order_id', 'account_id', 'status', 'created_at'],
    rows: canonical.orders.map(r => ({
      order_id: r.orderId,
      account_id: r.accountId,
      status: r.status,
      created_at: r.createdAt,
    })),
  });

  bundles.push({
    filename: 'order_items.csv',
    headers: ['order_item_id', 'order_id', 'sku', 'qty', 'price_cents'],
    rows: canonical.order_items.map(r => ({
      order_item_id: r.orderItemId,
      order_id: r.orderId,
      sku: r.sku,
      qty: r.qty,
      price_cents: r.priceCents,
    })),
  });

  bundles.push({
    filename: 'features.csv',
    headers: ['feature_id', 'code', 'name'],
    rows: canonical.features.map(r => ({
      feature_id: r.featureId,
      code: r.code,
      name: r.name,
    })),
  });

  bundles.push({
    filename: 'subscriber_features.csv',
    headers: ['subscriber_id', 'feature_id'],
    rows: canonical.subscriber_features.map(r => ({
      subscriber_id: r.subscriberId,
      feature_id: r.featureId,
    })),
  });

  bundles.push({
    filename: 'subscriber_feature_state.csv',
    headers: [
      'subscriber_feature_state_id',
      'subscriber_id',
      'feature_id',
      'effective_from',
      'effective_to',
      'provisioning_state',
      'source'
    ],
    rows: canonical.subscriber_feature_state.map(r => ({
      subscriber_feature_state_id: r.subscriberFeatureStateId,
      subscriber_id: r.subscriberId,
      feature_id: r.featureId,
      effective_from: r.effectiveFrom,
      effective_to: r.effectiveTo,
      provisioning_state: r.provisioningState,
      source: r.source,
    })),
  });

  bundles.push({
    filename: 'ticket_status_codes.csv',
    headers: ['code', 'description'],
    rows: canonical.ticket_status_codes.map(r => ({
      code: r.code,
      description: r.description,
    })),
  });

  bundles.push({
    filename: 'tickets.csv',
    headers: ['ticket_id', 'subscriber_id', 'status_code', 'opened_at', 'closed_at', 'summary'],
    rows: canonical.tickets.map(r => ({
      ticket_id: r.ticketId,
      subscriber_id: r.subscriberId,
      status_code: r.statusCode,
      opened_at: r.openedAt,
      closed_at: r.closedAt,
      summary: r.summary,
    })),
  });

  bundles.push({
    filename: 'notes.csv',
    headers: ['note_id', 'ref_type', 'ref_id', 'author', 'body', 'created_at'],
    rows: canonical.notes.map(r => ({
      note_id: r.noteId,
      ref_type: r.refType,
      ref_id: r.refId,
      author: r.author,
      body: r.body,
      created_at: r.createdAt,
    })),
  });

  bundles.push({
    filename: 'org_units.csv',
    headers: ['org_unit_id', 'name', 'parent_org_unit_id'],
    rows: canonical.org_units.map(r => ({
      org_unit_id: r.orgUnitId,
      name: r.name,
      parent_org_unit_id: r.parentOrgUnitId,
    })),
  });

  bundles.push({
    filename: 'plans.csv',
    headers: ['plan_id', 'code', 'name'],
    rows: canonical.plans.map(r => ({
      plan_id: r.planId,
      code: r.code,
      name: r.name,
    })),
  });

  bundles.push({
    filename: 'regions.csv',
    headers: ['region_id', 'code'],
    rows: canonical.regions.map(r => ({
      region_id: r.regionId,
      code: r.code,
    })),
  });

  bundles.push({
    filename: 'device_classes.csv',
    headers: ['device_class_id', 'code'],
    rows: canonical.device_classes.map(r => ({
      device_class_id: r.deviceClassId,
      code: r.code,
    })),
  });

  bundles.push({
    filename: 'rates.csv',
    headers: ['plan_id', 'region_id', 'device_class_id', 'rate_cents'],
    rows: canonical.rates.map(r => ({
      plan_id: r.planId,
      region_id: r.regionId,
      device_class_id: r.deviceClassId,
      rate_cents: r.rateCents,
    })),
  });

  bundles.push({
    filename: 'usage_records.csv',
    headers: ['usage_record_id', 'subscriber_id', 'ts', 'usage_type', 'units', 'rated_cents'],
    rows: canonical.usage_records.map(r => ({
      usage_record_id: r.usageRecordId,
      subscriber_id: r.subscriberId,
      ts: r.ts,
      usage_type: r.usageType,
      units: r.units,
      rated_cents: r.ratedCents,
    })),
  });

  return bundles;
}

// ---------- write outputs ----------
function generateAll(args) {
  const cfg = presets(args.size);
  const rng = mulberry32(args.seed);

  ensureDir(args.out);

  const manifestPath = path.join(args.out, 'manifest.json');
  if (fs.existsSync(manifestPath) && !args.overwrite) {
    throw new Error(`Refusing to overwrite existing dataset (manifest exists): ${manifestPath} (use --overwrite)`);
  }

  const canonical = generateCanonical(rng, cfg);
  const mongoNormalized = toMongoNormalized(canonical);
  const mongoOptimized = toMongoOptimized(canonical, mongoNormalized, cfg);

  const outCanonical = path.join(args.out, 'canonical');
  const outMongoNorm = path.join(args.out, 'mongo_normalized');
  const outMongoOpt = path.join(args.out, 'mongo_optimized');
  const outPgData = path.join(args.out, 'postgres', 'data');

  const entityOrder = [
    'accounts',
    'subscribers',
    'subscriber_profiles',
    'devices',
    'device_events',
    'orders',
    'order_items',
    'features',
    'subscriber_features',
    'subscriber_feature_state',
    'ticket_status_codes',
    'tickets',
    'notes',
    'org_units',
    'plans',
    'regions',
    'device_classes',
    'rates',
    'usage_records',
  ];

  // canonical NDJSON
  if (args.only === 'canonical' || args.only === 'all') {
    ensureDir(outCanonical);
    for (const name of entityOrder) {
      const rows = canonical[name] || [];
      writeNdjson(path.join(outCanonical, `${name}.ndjson`), rows);
    }
  }

  // mongo_normalized NDJSON
  if (args.only === 'mongo_normalized' || args.only === 'all') {
    ensureDir(outMongoNorm);
    for (const name of entityOrder) {
      const rows = mongoNormalized[name] || [];
      writeNdjson(path.join(outMongoNorm, `${name}.ndjson`), rows);
    }
  }

  // mongo_optimized NDJSON (subset by design)
  if (args.only === 'mongo_optimized' || args.only === 'all') {
    ensureDir(outMongoOpt);
    for (const name of entityOrder) {
      if (!mongoOptimized[name]) continue;
      writeNdjson(path.join(outMongoOpt, `${name}.ndjson`), mongoOptimized[name]);
    }
  }

  // postgres CSV
  if (args.only === 'postgres' || args.only === 'all') {
    ensureDir(outPgData);
    const bundles = toPostgresCsvBundles(canonical);
    for (const b of bundles) {
      const csv = toCsv(b.headers, b.rows);
      safeWrite(path.join(outPgData, b.filename), csv, true);
    }
  }

  // Manifest (always)
  const counts = {};
  for (const name of entityOrder) {
    counts[name] = (canonical[name] || []).length;
  }

  const manifest = {
    datasetVersion: DATASET_VERSION,
    generatedAt: new Date().toISOString(),
    seed: args.seed,
    size: args.size,
    presets: cfg,
    equivalenceRules: {
      idStrategy: 'integers',
      canonicalIsTruth: true,
      mongoNormalizedMirrorsCanonical: true,
      mongoOptimizedEmbeds: {
        subscribers: ['profile', cfg.embedFeatureCodes ? 'featureCodes' : null].filter(Boolean),
        orders: ['items'],
      },
      usageUnits: 'voice=seconds, sms=messages, data=KB',
      postgresCsv: {
        format: 'CSV header, Postgres COPY-compatible',
        jsonbColumns: ['subscriber_profiles.preferences', 'device_events.payload'],
      }
    },
    counts,
  };

  safeWrite(manifestPath, JSON.stringify(manifest, null, 2) + '\n', true);

  return manifest;
}

// ---------- main ----------
(function main() {
  try {
    const args = parseArgs(process.argv);
    const manifest = generateAll(args);
    console.log(JSON.stringify({ ok: true, out: args.out, manifest: 'manifest.json', datasetVersion: manifest.datasetVersion }));
  } catch (err) {
    console.error(`ERROR: ${err.message}`);
    process.exit(1);
  }
})();
