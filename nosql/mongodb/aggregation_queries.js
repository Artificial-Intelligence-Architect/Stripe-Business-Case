// ============================================================
// NoSQL/MongoDB: Analytical Aggregation Queries
// ============================================================

// Connect to the target database (to be adapted)
// use stripe_nosql;

// ------------------------------------------------------------
// 1. Top 10 at-risk merchants (last hour)
//    Based on fraud signals from recent transactions.
// ------------------------------------------------------------
db.fraud_events.aggregate([
  {
    $match: {
      timestamp: { $gte: new Date(Date.now() - 3600 * 1000) },
      "fraud_signals.score": { $gte: 0.7 }
    }
  },
  {
    $group: {
      _id: "$merchant_id",
      avg_fraud_score:    { $avg: "$fraud_signals.score" },
      flagged_count:      { $sum: 1 },
      total_amount_avg:   { $avg: "$ml_features.avg_txn_amount_30d" },
      geo_anomaly_count:  { $sum: { $cond: ["$fraud_signals.geo_anomaly", 1, 0] } },
      velocity_flag_count:{ $sum: { $cond: ["$fraud_signals.velocity_flag", 1, 0] } }
    }
  },
  { $sort: { avg_fraud_score: -1 } },
  { $limit: 10 },
  {
    $project: {
      merchant_id:         "$_id",
      avg_fraud_score:     { $round: ["$avg_fraud_score", 3] },
      flagged_count:       1,
      geo_anomaly_pct:     {
        $round: [{ $divide: ["$geo_anomaly_count", "$flagged_count"] }, 2]
      },
      velocity_flag_pct:   {
        $round: [{ $divide: ["$velocity_flag_count", "$flagged_count"] }, 2]
      }
    }
  }
]);

// ------------------------------------------------------------
// 2. User sessions that led to a fraudulent transaction
//    within the following 5 minutes.
// ------------------------------------------------------------
db.user_sessions.aggregate([
  {
    $lookup: {
      from: "fraud_events",
      let: { cid: "$customer_id", sess_end: "$ended_at" },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq:  ["$customer_id", "$$cid"] },
                { $gte: ["$timestamp", "$$sess_end"] },
                { $lte: ["$timestamp", { $add: ["$$sess_end", 300000] }] } // 5 minutes later
              ]
            },
            decision: "flagged"
          }
        }
      ],
      as: "fraud_after_session"
    }
  },
  { $match: { "fraud_after_session.0": { $exists: true } } },
  {
    $project: {
      session_id: 1,
      customer_id: 1,
      duration_sec: 1,
      device_type: "$device.type",
      events_count: { $size: "$events" },
      checkout_time_ms: {
        $sum: {
          $map: {
            input: {
              $filter: {
                input: "$events",
                cond: { $regexMatch: { input: "$$this.url", regex: "/checkout" } }
              }
            },
            as: "e",
            in: "$$e.duration_ms"
          }
        }
      },
      fraud_score: { $arrayElemAt: ["$fraud_after_session.fraud_signals.score", 0] }
    }
  }
]);

// ------------------------------------------------------------
// 3. Error distribution by service (last 24 hours)
// ------------------------------------------------------------
db.app_logs.aggregate([
  {
    $match: {
      level: { $in: ["ERROR", "CRITICAL"] },
      timestamp: { $gte: new Date(Date.now() - 24 * 3600 * 1000) }
    }
  },
  {
    $group: {
      _id: { service: "$service", level: "$level" },
      count: { $sum: 1 },
      sample_messages: { $addToSet: { $substr: ["$message", 0, 100] } }
    }
  },
  { $sort: { count: -1 } },
  {
    $group: {
      _id: "$_id.service",
      total_errors: { $sum: "$count" },
      by_level: {
        $push: {
          level: "$_id.level",
          count: "$count"
        }
      },
      examples: { $first: "$sample_messages" }
    }
  },
  { $sort: { total_errors: -1 } }
]);

// ------------------------------------------------------------
// 4. Average fraud score by model version
//    (ML model performance evaluation)
// ------------------------------------------------------------
db.fraud_events.aggregate([
  {
    $group: {
      _id: "$model_version",
      avg_score: { $avg: "$fraud_signals.score" },
      count: { $sum: 1 },
      flagged_count: { $sum: { $cond: [{ $eq: ["$decision", "flagged"] }, 1, 0] } },
      blocked_count: { $sum: { $cond: [{ $eq: ["$decision", "blocked"] }, 1, 0] } }
    }
  },
  {
    $project: {
      model_version: "$_id",
      avg_score: { $round: ["$avg_score", 3] },
      count: 1,
      flagged_pct: { $round: [{ $divide: ["$flagged_count", "$count"] }, 2] },
      blocked_pct: { $round: [{ $divide: ["$blocked_count", "$count"] }, 2] }
    }
  },
  { $sort: { count: -1 } }
]);

// ------------------------------------------------------------
// 5. IP anomaly detection (more than 100 errors in 24 hours)
// ------------------------------------------------------------
db.app_logs.aggregate([
  {
    $match: {
      timestamp: { $gte: new Date(Date.now() - 24 * 3600 * 1000) },
      level: { $in: ["ERROR", "CRITICAL"] }
    }
  },
  {
    $group: {
      _id: "$context.merchant_id",
      ip: { $first: "$host" }, // simplified usage (in production, use the source IP)
      error_count: { $sum: 1 },
      distinct_services: { $addToSet: "$service" }
    }
  },
  { $match: { error_count: { $gt: 100 } } },
  { $sort: { error_count: -1 } }
]);