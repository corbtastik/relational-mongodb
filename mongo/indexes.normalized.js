// mongo/indexes.normalized.js
// Creates indexes for the "mongo_normalized" dataset shape.
// Assumes documents use camelCase fields (subscriberId, accountId, openedAt, etc.)
// and notes use refType/refId (not ref.type/ref.id).

(function () {
  // subscribers
  db.subscribers.createIndex({ msisdn: 1 }, { unique: true, name: "msisdn_unique" });
  db.subscribers.createIndex({ accountId: 1 }, { name: "accountId_1" });

  // subscriber_profiles (1:1 reference)
  db.subscriber_profiles.createIndex({ subscriberId: 1 }, { unique: true, name: "subscriberId_unique" });

  // devices
  db.devices.createIndex({ imei: 1 }, { unique: true, name: "imei_unique" });
  db.devices.createIndex({ subscriberId: 1 }, { name: "subscriberId_1" });

  // device_events (1:N medium)
  db.device_events.createIndex({ deviceId: 1, ts: -1 }, { name: "deviceId_1_ts_-1" });

  // orders + items (1:N small)
  db.orders.createIndex({ accountId: 1, createdAt: -1 }, { name: "accountId_1_createdAt_-1" });
  db.order_items.createIndex({ orderId: 1 }, { name: "orderId_1" });

  // features
  db.features.createIndex({ code: 1 }, { unique: true, name: "feature_code_unique" });

  // subscriber_features (M:N bridge)
  db.subscriber_features.createIndex({ subscriberId: 1 }, { name: "subscriberId_1" });
  db.subscriber_features.createIndex({ featureId: 1 }, { name: "featureId_1" });
  db.subscriber_features.createIndex(
    { subscriberId: 1, featureId: 1 },
    { unique: true, name: "subscriberId_1_featureId_1_unique" }
  );

  // subscriber_feature_state (associative entity with attributes)
  db.subscriber_feature_state.createIndex({ subscriberId: 1, effectiveFrom: -1 }, { name: "subscriberId_1_effectiveFrom_-1" });
  db.subscriber_feature_state.createIndex({ featureId: 1 }, { name: "featureId_1" });

  // tickets + status codes
  // ticket_status_codes uses _id = code in mongo_normalized; _id is already indexed.
  db.tickets.createIndex({ subscriberId: 1, openedAt: -1 }, { name: "subscriberId_1_openedAt_-1" });
  db.tickets.createIndex({ statusCode: 1 }, { name: "statusCode_1" });

  // notes (polymorphic)
  db.notes.createIndex({ refType: 1, refId: 1, createdAt: -1 }, { name: "refType_1_refId_1_createdAt_-1" });

  // org_units (hierarchy)
  db.org_units.createIndex({ parentOrgUnitId: 1 }, { name: "parentOrgUnitId_1" });

  // rates (ternary relationship)
  db.rates.createIndex(
    { planId: 1, regionId: 1, deviceClassId: 1 },
    { unique: true, name: "planId_1_regionId_1_deviceClassId_1_unique" }
  );

  // usage_records (1:N large)
  db.usage_records.createIndex({ subscriberId: 1, ts: -1 }, { name: "subscriberId_1_ts_-1" });

  print("✅ Indexes created (normalized).");
})();
