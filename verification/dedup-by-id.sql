-- Pattern: deduplication ordered by order_id, not a time attribute. Run with
-- FLINK_VERSION=2.3-SNAPSHOT: Flink 2.3.0 wrongly marks this Rank's output as insert-only, which
-- FLINK-40368 fixes in 2.3.1. With the fix, an append-only sink is rejected, so the sink here is
-- upsert-kafka.

CREATE TABLE orders (
  order_id          BIGINT,
  store_id          INT,
  pizza_id          INT,
  channel           STRING,
  total             DECIMAL(10, 2),
  delivery_address  STRING,
  event_time        TIMESTAMP(3),
  proc_time         AS PROCTIME(),
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
  'connector' = 'kafka', 'topic' = 'orders',
  'properties.bootstrap.servers' = 'localhost:9092',
  'scan.startup.mode' = 'earliest-offset', 'format' = 'json'
);

CREATE TABLE deduped_orders (
  order_id    BIGINT,
  store_id    INT,
  total       DECIMAL(10, 2),
  event_time  TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka', 'topic' = 'deduped_orders',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'json', 'value.format' = 'json'
);

EXPLAIN CHANGELOG_MODE
INSERT INTO deduped_orders
SELECT order_id, store_id, total, event_time
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY order_id
                       ORDER BY order_id ASC) AS row_num
  FROM orders)
WHERE row_num = 1;
