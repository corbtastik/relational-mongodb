-- seed/postgres/load.sql
-- Load CarrierOps CSVs into Postgres using COPY in dependency-safe order.
--
-- How to run (example):
--   psql -d carrierops -f seed/postgres/tables.sql
--   psql -d carrierops -v data_dir='/ABS/PATH/TO/relational-mongodb/seed/postgres/data' -f seed/postgres/load.sql
--
-- NOTE: data_dir MUST be an absolute path readable by the Postgres server process for COPY.
-- If you're running Postgres locally in Docker and COPY can't see your host path,
-- use \copy instead (client-side). (We can add a \copy variant if you want.)

\set ON_ERROR_STOP on

-- Default (override on command line with -v data_dir=...)
\if :{?data_dir}
\else
  \set data_dir '/ABS/PATH/TO/relational-mongodb/seed/postgres/data'
\endif

-- Build full paths as psql variables (then use COPY ... FROM :'var')
\set accounts_csv                 :data_dir '/accounts.csv'
\set subscribers_csv              :data_dir '/subscribers.csv'
\set subscriber_profiles_csv      :data_dir '/subscriber_profiles.csv'
\set devices_csv                  :data_dir '/devices.csv'
\set device_events_csv            :data_dir '/device_events.csv'
\set orders_csv                   :data_dir '/orders.csv'
\set order_items_csv              :data_dir '/order_items.csv'
\set features_csv                 :data_dir '/features.csv'
\set subscriber_features_csv      :data_dir '/subscriber_features.csv'
\set subscriber_feature_state_csv :data_dir '/subscriber_feature_state.csv'
\set ticket_status_codes_csv      :data_dir '/ticket_status_codes.csv'
\set tickets_csv                  :data_dir '/tickets.csv'
\set notes_csv                    :data_dir '/notes.csv'
\set org_units_csv                :data_dir '/org_units.csv'
\set plans_csv                    :data_dir '/plans.csv'
\set regions_csv                  :data_dir '/regions.csv'
\set device_classes_csv           :data_dir '/device_classes.csv'
\set rates_csv                    :data_dir '/rates.csv'
\set usage_records_csv            :data_dir '/usage_records.csv'

BEGIN;

-- Fast reset (FK-safe)
TRUNCATE TABLE
  device_events,
  usage_records,
  rates,
  notes,
  tickets,
  subscriber_feature_state,
  subscriber_features,
  order_items,
  orders,
  devices,
  subscriber_profiles,
  subscribers,
  accounts,
  org_units,
  ticket_status_codes,
  features,
  device_classes,
  regions,
  plans
RESTART IDENTITY CASCADE;

-- ---- Dimension / lookup-ish tables first ----
COPY ticket_status_codes (code, description)
FROM :'ticket_status_codes_csv'
WITH (FORMAT csv, HEADER true);

COPY plans (plan_id, code, name)
FROM :'plans_csv'
WITH (FORMAT csv, HEADER true);

COPY regions (region_id, code)
FROM :'regions_csv'
WITH (FORMAT csv, HEADER true);

COPY device_classes (device_class_id, code)
FROM :'device_classes_csv'
WITH (FORMAT csv, HEADER true);

COPY features (feature_id, code, name)
FROM :'features_csv'
WITH (FORMAT csv, HEADER true);

COPY org_units (org_unit_id, name, parent_org_unit_id)
FROM :'org_units_csv'
WITH (FORMAT csv, HEADER true);

-- ---- Core entities ----
COPY accounts (account_id, account_number, name, billing_region_code, status, created_at)
FROM :'accounts_csv'
WITH (FORMAT csv, HEADER true);

COPY subscribers (subscriber_id, account_id, msisdn, status, created_at)
FROM :'subscribers_csv'
WITH (FORMAT csv, HEADER true);

COPY subscriber_profiles (subscriber_id, first_name, last_name, email, dob, pii_last4_ssn, preferences, updated_at)
FROM :'subscriber_profiles_csv'
WITH (FORMAT csv, HEADER true);

COPY devices (device_id, subscriber_id, imei, model, created_at)
FROM :'devices_csv'
WITH (FORMAT csv, HEADER true);

-- ---- Orders (1:N small) ----
COPY orders (order_id, account_id, status, created_at)
FROM :'orders_csv'
WITH (FORMAT csv, HEADER true);

COPY order_items (order_item_id, order_id, sku, qty, price_cents)
FROM :'order_items_csv'
WITH (FORMAT csv, HEADER true);

-- ---- M:N + associative ----
COPY subscriber_features (subscriber_id, feature_id)
FROM :'subscriber_features_csv'
WITH (FORMAT csv, HEADER true);

COPY subscriber_feature_state (
  subscriber_feature_state_id,
  subscriber_id,
  feature_id,
  effective_from,
  effective_to,
  provisioning_state,
  source
)
FROM :'subscriber_feature_state_csv'
WITH (FORMAT csv, HEADER true);

-- ---- Tickets + notes ----
COPY tickets (ticket_id, subscriber_id, status_code, opened_at, closed_at, summary)
FROM :'tickets_csv'
WITH (FORMAT csv, HEADER true);

COPY notes (note_id, ref_type, ref_id, author, body, created_at)
FROM :'notes_csv'
WITH (FORMAT csv, HEADER true);

-- ---- Ternary relationship ----
COPY rates (plan_id, region_id, device_class_id, rate_cents)
FROM :'rates_csv'
WITH (FORMAT csv, HEADER true);

-- ---- Events / time-series-ish ----
COPY device_events (device_event_id, device_id, event_type, ts, payload)
FROM :'device_events_csv'
WITH (FORMAT csv, HEADER true);

COPY usage_records (usage_record_id, subscriber_id, ts, usage_type, units, rated_cents)
FROM :'usage_records_csv'
WITH (FORMAT csv, HEADER true);

COMMIT;
