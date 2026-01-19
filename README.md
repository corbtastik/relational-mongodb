# Relational MongoDB (CarrierOps)

This repo is a practical “show, don’t tell” demo that **MongoDB is relational** — it supports the same relationship patterns you already know (including joins via `$lookup`) — but it also lets you model data in a more **workload-friendly shape** (embedding where it makes sense).

We use a single telco-ish domain called **CarrierOps** and generate one deterministic dataset, then load it into MongoDB in **two shapes** so you can compare them apples-to-apples:

- **Normalized shape** (RDBMS-style collections): `carrierops_normalized`
- **Optimized shape** (document-first embedding where it helps): `carrierops_optimized`

> Next: we’ll load the *same truth* into Postgres via CSV + `COPY` so the comparisons are side-by-side.

---

## Repo layout (the parts you’ll use)

- `tools/generate-dataset.js`  
  Deterministic dataset generator (NDJSON + Postgres CSV).
- `scripts/gen-dataset.sh`  
  Wrapper to generate datasets (`S|M|L`, seed, overwrite).
- `scripts/load-mongo.sh`  
  Loads NDJSON into MongoDB + creates indexes.
- `mongo/indexes.normalized.js`  
  Indexes for `carrierops_normalized`.
- `mongo/indexes.optimized.js`  
  Indexes for `carrierops_optimized`.
- `seed/`  
  Generated outputs (NDJSON, CSV, manifest). Safe to delete and regenerate.

---

## Prereqs

You’ll need:

- **Node.js** (to generate data)
- **mongoimport** (MongoDB Database Tools)
- **mongosh**
- A MongoDB to connect to (local, container, or Atlas)

Quick checks:

```bash
node -v
mongosh --version
mongoimport --version
```

---

## Step 1 — Generate the dataset (deterministic)

From repo root:

```bash
./scripts/gen-dataset.sh --size S --seed 42 --overwrite
```

Outputs:

- `seed/canonical/*.ndjson`
- `seed/mongo_normalized/*.ndjson`
- `seed/mongo_optimized/*.ndjson`
- `seed/postgres/data/*.csv`
- `seed/manifest.json`

### Dataset sizes

- `S` = small blog-friendly dataset (fast, readable)
- `M` / `L` = larger datasets (for perf / “real-ish” query testing)

Same `--seed` ⇒ same dataset across machines.

---

## Step 2 — Load into MongoDB (two shapes)

### Set your MongoDB URI
Use a URI **without** a database path (the loader chooses DBs by name). If you authenticate against `admin`, keep it in `authSource`.

```bash
export MONGODB_URI='mongodb://USERNAME:PASSWORD@127.0.0.1:37017/?authSource=admin'
```

### Load both shapes (recommended)
This will import and build indexes for both DBs:

```bash
./scripts/load-mongo.sh --uri "$MONGODB_URI" --shape both --drop
```

Creates:

- `carrierops_normalized`
- `carrierops_optimized`

### Load just one shape
```bash
./scripts/load-mongo.sh --uri "$MONGODB_URI" --shape normalized --drop
./scripts/load-mongo.sh --uri "$MONGODB_URI" --shape optimized  --drop
```

---

## Sanity checks

```bash
mongosh "$MONGODB_URI" --eval 'db.getMongo().getDB("carrierops_normalized").subscribers.countDocuments()'
mongosh "$MONGODB_URI" --eval 'db.getMongo().getDB("carrierops_optimized").orders.countDocuments()'
```

---

## What “two shapes” means

### `carrierops_normalized` (RDBMS-like)
Collections are split like tables:
- `subscribers` and `subscriber_profiles` are separate (1:1)
- `orders` and `order_items` are separate (1:N small)
- Relational patterns are represented explicitly, and you use `$lookup` when you want joins

### `carrierops_optimized` (document-first)
Documents embed what’s commonly read together:
- `subscribers.profile` is embedded (1:1)
- `orders.items` is embedded (1:N small)
- Some relationships remain as references/bridge collections where that’s the right shape

Same truth, different shape.

---

## Example: 1:1 (Subscriber ↔ SubscriberProfile)

### Normalized: join with `$lookup`
```bash
mongosh "$MONGODB_URI/carrierops_normalized" --eval '
  const msisdn = db.subscribers.findOne({}, { msisdn: 1 }).msisdn;

  const doc = db.subscribers.aggregate([
    { $match: { msisdn } },
    {
      $lookup: {
        from: "subscriber_profiles",
        localField: "subscriberId",
        foreignField: "subscriberId",
        as: "profile"
      }
    },
    { $set: { profile: { $first: "$profile" } } },
    { $project: { _id: 1, subscriberId: 1, msisdn: 1, status: 1, "profile.email": 1, "profile.preferences": 1 } }
  ]).toArray()[0];

  printjson(doc);
'
```

### Optimized: read the embedded profile (no join)
```bash
mongosh "$MONGODB_URI/carrierops_optimized" --eval '
  const msisdn = db.subscribers.findOne({}, { msisdn: 1 }).msisdn;

  const doc = db.subscribers.findOne(
    { msisdn },
    { _id: 1, subscriberId: 1, msisdn: 1, status: 1, "profile.email": 1, "profile.preferences": 1 }
  );

  printjson(doc);
'
```

---

## Common gotchas

### 1) “permission denied” running scripts
```bash
chmod u+x ./scripts/gen-dataset.sh ./scripts/load-mongo.sh
```

### 2) mongoimport error: “Cannot specify different database in connection URI and command-line option”
Don’t include a DB path in your URI (e.g. avoid `/admin` in the URI path). Use `?authSource=admin` instead.

Good:
- `mongodb://...:37017/?authSource=admin`

Risky:
- `mongodb://...:37017/admin?authSource=admin`

---

## What’s next

- Load the same dataset into Postgres using:
  - `seed/postgres/tables.sql`
  - `seed/postgres/load.sql`
- Add “blog queries” that run on:
  - Postgres (SQL)
  - MongoDB normalized (`$lookup` / references)
  - MongoDB optimized (embedded reads)

---

## Notes on IDs and determinism

- IDs are **integers** across “main entities” for clarity (`_id = <int>` in MongoDB where applicable).
- The dataset generator is deterministic:
  - Same `--seed` + same `--size` ⇒ same outputs

---

## License

MIT (or your preferred license—add `LICENSE` if/when you want).
