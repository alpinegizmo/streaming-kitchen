-- Pattern: event correlation (interval join)

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

CREATE TABLE deliveries (
  order_id    BIGINT,
  driver_id   INT,
  event_time  TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
  'connector' = 'kafka', 'topic' = 'deliveries',
  'properties.bootstrap.servers' = 'localhost:9092',
  'scan.startup.mode' = 'earliest-offset', 'format' = 'json'
);

CREATE TABLE late_orders (
  order_id    BIGINT,
  store_id    INT,
  ordered_at  TIMESTAMP(3)
) WITH (
  'connector' = 'kafka', 'topic' = 'late_orders',
  'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json'
);

EXPLAIN CHANGELOG_MODE
INSERT INTO late_orders
SELECT o.order_id, o.store_id, o.event_time AS ordered_at
FROM orders o
LEFT JOIN deliveries d
  ON o.order_id = d.order_id
 AND d.event_time BETWEEN o.event_time
                      AND o.event_time + INTERVAL '45' MINUTE
WHERE d.order_id IS NULL;
