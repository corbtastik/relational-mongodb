// mongo/indexes.optimized.js
// Creates indexes for the "mongo_optimized" dataset shape.
//
// Key differences vs normalized:
// - subscribers embed "profile" and may include "featureCodes" (if generator enabled it)
// - orders embed "items"
// - subscriber_profiles and order_items collections do not exist in optimized output
//
// Notes still use refType/refId in the generator today.

(function () {
  // subscribers (profile embedded)
  db.subscribers.createIndex({ msisdn: 1 }, { unique: true, name: "msisdn_unique" });
  db.subscribers.createIndex({ accountId: 1 }, { name: "accountId_1" });

  // If featureCodes exist and you want to query "who has HOTSPOT"
  // this multikey index helps.
  db.subscribers.createIndex({ featureCodes: 1 }, { name: "featureCodes_1" });

  // devices
  db.devices.createIndex({ imei: 1 }, { unique: true, name: "imei_unique" });
  db.devices.createIndex({ subscriberId: 1 }, { name: "subscriberId_1" });

  // device_events
  db.device_events.createIndex({ deviceId: 1, ts: -1 }, { name: "deviceId_1_ts_-1" });

  // orders (items embedded)
  db.orders.createIndex({ accountId: 1, createdAt: -1 }, { name: "accountId_1_createdAt_-1" });
  // Optional: query for orders containing an SKU
  db.orders.createIndex({ "items.sku": 1 }, { name: "items_sku_1" });

  // features
  db.features.createIndex({ code: 1 }, { unique: true, name: "feature_code_unique" });

  // subscriber_features + subscriber_feature_state still exist (we didn't embed those)
  db.subscriber_features.createIndex({ subscriberId: 1 }, { name: "subscriberId_1" });
  db.subscriber_features.createIndex({ featureId: 1 }, { name: "featureId_1" });
  db.subscriber_features.createIndex(
    { subscriberId: 1, featureId: 1 },
    { unique: true, name: "subscriberId_1_featureId_1_unique" }
  );

  db.subscriber_feature_state.createIndex({ subscriberId: 1, effectiveFrom: -1 }, { name: "subscriberId_1_effectiveFrom_-1" });
  db.subscriber_feature_state.createIndex({ featureId: 1 }, { name: "featureId_1" });

  // tickets
  db.tickets.createIndex({ subscriberId: 1, openedAt: -1 }, { name: "subscriberId_1_openedAt_-1" });
  db.tickets.createIndex({ statusCode: 1 }, { name: "statusCode_1" });

  // notes
  db.notes.createIndex({ refType: 1, refId: 1, createdAt: -1 }, { name: "refType_1_refId_1_createdAt_-1" });

  // org_units
  db.org_units.createIndex({ parentOrgUnitId: 1 }, { name: "parentOrgUnitId_1" });

  // rates
  db.rates.createIndex(
    { planId: 1, regionId: 1, deviceClassId: 1 },
    { unique: true, name: "planId_1_regionId_1_deviceClassId_1_unique" }
  );

  // usage_records
  db.usage_records.createIndex({ subscriberId: 1, ts: -1 }, { name: "subscriberId_1_ts_-1" });

  print("✅ Indexes created (optimized).");
})();
