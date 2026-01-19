-- seed/postgres/tables.sql
-- CarrierOps (normalized) — Postgres DDL
--
-- Notes:
-- - Integer IDs everywhere (BIGINT) for apples-to-apples with MongoDB int _id strategy.
-- - usage_records.units is generic:
--     voice -> seconds, sms -> messages, data -> KB
-- - Status fields are TEXT for blog simplicity (could be enums in real systems).

BEGIN;

-- ---------- core ----------
CREATE TABLE IF NOT EXISTS accounts (
  account_id           BIGINT PRIMARY KEY,
  account_number       TEXT NOT NULL UNIQUE,
  name                 TEXT NOT NULL,
  billing_region_code  TEXT NOT NULL,
  status               TEXT NOT NULL,
  created_at           TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS subscribers (
  subscriber_id  BIGINT PRIMARY KEY,
  account_id     BIGINT NOT NULL REFERENCES accounts(account_id) ON DELETE RESTRICT,
  msisdn         TEXT NOT NULL UNIQUE,
  status         TEXT NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS subscribers_account_id_idx
  ON subscribers(account_id);

-- 1:1 extension
CREATE TABLE IF NOT EXISTS subscriber_profiles (
  subscriber_id   BIGINT PRIMARY KEY REFERENCES subscribers(subscriber_id) ON DELETE CASCADE,
  first_name      TEXT,
  last_name       TEXT,
  email           TEXT,
  dob             DATE,
  pii_last4_ssn   TEXT,
  preferences     JSONB,
  updated_at      TIMESTAMPTZ NOT NULL
);

-- ---------- devices + events ----------
CREATE TABLE IF NOT EXISTS devices (
  device_id      BIGINT PRIMARY KEY,
  subscriber_id  BIGINT NOT NULL REFERENCES subscribers(subscriber_id) ON DELETE RESTRICT,
  imei           TEXT NOT NULL UNIQUE,
  model          TEXT NOT NULL,
  created_at     TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS devices_subscriber_id_idx
  ON devices(subscriber_id);

CREATE TABLE IF NOT EXISTS device_events (
  device_event_id  BIGINT PRIMARY KEY,
  device_id        BIGINT NOT NULL REFERENCES devices(device_id) ON DELETE CASCADE,
  event_type       TEXT NOT NULL,
  ts               TIMESTAMPTZ NOT NULL,
  payload          JSONB
);

CREATE INDEX IF NOT EXISTS device_events_device_ts_idx
  ON device_events(device_id, ts DESC);

-- ---------- orders (1:N small) ----------
CREATE TABLE IF NOT EXISTS orders (
  order_id    BIGINT PRIMARY KEY,
  account_id  BIGINT NOT NULL REFERENCES accounts(account_id) ON DELETE RESTRICT,
  status      TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS orders_account_id_idx
  ON orders(account_id);

CREATE TABLE IF NOT EXISTS order_items (
  order_item_id  BIGINT PRIMARY KEY,
  order_id       BIGINT NOT NULL REFERENCES orders(order_id) ON DELETE CASCADE,
  sku            TEXT NOT NULL,
  qty            INT NOT NULL CHECK (qty > 0),
  price_cents    INT NOT NULL CHECK (price_cents >= 0)
);

CREATE INDEX IF NOT EXISTS order_items_order_id_idx
  ON order_items(order_id);

-- ---------- features (M:N) ----------
CREATE TABLE IF NOT EXISTS features (
  feature_id  BIGINT PRIMARY KEY,
  code        TEXT NOT NULL UNIQUE,
  name        TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS subscriber_features (
  subscriber_id  BIGINT NOT NULL REFERENCES subscribers(subscriber_id) ON DELETE CASCADE,
  feature_id     BIGINT NOT NULL REFERENCES features(feature_id) ON DELETE RESTRICT,
  PRIMARY KEY (subscriber_id, feature_id)
);

CREATE INDEX IF NOT EXISTS subscriber_features_feature_id_idx
  ON subscriber_features(feature_id);

-- associative entity (M:N with attributes / history rows)
CREATE TABLE IF NOT EXISTS subscriber_feature_state (
  subscriber_feature_state_id  BIGINT PRIMARY KEY,
  subscriber_id                BIGINT NOT NULL REFERENCES subscribers(subscriber_id) ON DELETE CASCADE,
  feature_id                   BIGINT NOT NULL REFERENCES features(feature_id) ON DELETE RESTRICT,
  effective_from               TIMESTAMPTZ NOT NULL,
  effective_to                 TIMESTAMPTZ NULL,
  provisioning_state           TEXT NOT NULL,
  source                       TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS subscriber_feature_state_sub_effective_idx
  ON subscriber_feature_state(subscriber_id, effective_from DESC);

-- ---------- lookup + tickets ----------
CREATE TABLE IF NOT EXISTS ticket_status_codes (
  code         TEXT PRIMARY KEY,
  description  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tickets (
  ticket_id      BIGINT PRIMARY KEY,
  subscriber_id  BIGINT NOT NULL REFERENCES subscribers(subscriber_id) ON DELETE RESTRICT,
  status_code    TEXT NOT NULL REFERENCES ticket_status_codes(code) ON DELETE RESTRICT,
  opened_at      TIMESTAMPTZ NOT NULL,
  closed_at      TIMESTAMPTZ NULL,
  summary        TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS tickets_subscriber_opened_idx
  ON tickets(subscriber_id, opened_at DESC);

CREATE INDEX IF NOT EXISTS tickets_status_code_idx
  ON tickets(status_code);

-- ---------- notes (polymorphic association) ----------
CREATE TABLE IF NOT EXISTS notes (
  note_id     BIGINT PRIMARY KEY,
  ref_type    TEXT NOT NULL,      -- 'subscriber' | 'order' | 'ticket'
  ref_id      BIGINT NOT NULL,
  author      TEXT NOT NULL,
  body        TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS notes_ref_idx
  ON notes(ref_type, ref_id, created_at DESC);

-- ---------- org units (self-reference hierarchy) ----------
CREATE TABLE IF NOT EXISTS org_units (
  org_unit_id         BIGINT PRIMARY KEY,
  name                TEXT NOT NULL,
  parent_org_unit_id  BIGINT NULL REFERENCES org_units(org_unit_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS org_units_parent_idx
  ON org_units(parent_org_unit_id);

-- ---------- plans / regions / device_classes / rates (ternary) ----------
CREATE TABLE IF NOT EXISTS plans (
  plan_id  BIGINT PRIMARY KEY,
  code     TEXT NOT NULL UNIQUE,
  name     TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS regions (
  region_id  BIGINT PRIMARY KEY,
  code       TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS device_classes (
  device_class_id  BIGINT PRIMARY KEY,
  code             TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS rates (
  plan_id          BIGINT NOT NULL REFERENCES plans(plan_id) ON DELETE RESTRICT,
  region_id        BIGINT NOT NULL REFERENCES regions(region_id) ON DELETE RESTRICT,
  device_class_id  BIGINT NOT NULL REFERENCES device_classes(device_class_id) ON DELETE RESTRICT,
  rate_cents       INT NOT NULL CHECK (rate_cents >= 0),
  PRIMARY KEY (plan_id, region_id, device_class_id)
);

-- ---------- usage records (1:N large) ----------
CREATE TABLE IF NOT EXISTS usage_records (
  usage_record_id  BIGINT PRIMARY KEY,
  subscriber_id    BIGINT NOT NULL REFERENCES subscribers(subscriber_id) ON DELETE RESTRICT,
  ts               TIMESTAMPTZ NOT NULL,
  usage_type       TEXT NOT NULL,         -- voice/sms/data
  units            INT NOT NULL CHECK (units >= 0),
  rated_cents      INT NOT NULL CHECK (rated_cents >= 0)
);

CREATE INDEX IF NOT EXISTS usage_records_subscriber_ts_idx
  ON usage_records(subscriber_id, ts DESC);

COMMIT;
