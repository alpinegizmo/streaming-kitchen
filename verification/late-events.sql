-- Pattern: late events to a dead letter queue, with a statement set

CREATE TABLE orders (
  order_id          BIGINT,
  store_id          INT,
  pizza_id          INT,
  channel           STRING,
  total             DECIMAL(10, 2),
  delivery_address  STRING,
  event_time        TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
  'connector' = 'kafka', 'topic' = 'orders',
  'properties.bootstrap.servers' = 'localhost:9092',
  'scan.startup.mode' = 'earliest-offset', 'format' = 'json'
);

CREATE TABLE store_sales (
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  store_id      INT,
  orders        BIGINT,
  sales         DECIMAL(38, 2)
) WITH (
  'connector' = 'kafka', 'topic' = 'store_sales',
  'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json'
);

CREATE TABLE late_orders (
  order_id          BIGINT,
  store_id          INT,
  pizza_id          INT,
  channel           STRING,
  total             DECIMAL(10, 2),
  delivery_address  STRING,
  event_time        TIMESTAMP(3)
) WITH (
  'connector' = 'kafka', 'topic' = 'late_orders',
  'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json'
);

-- The approach from the flink-intro talk: one definition of late, used by both branches.
EXPLAIN CHANGELOG_MODE STATEMENT SET
BEGIN

INSERT INTO late_orders
SELECT * FROM orders
WHERE event_time <= CURRENT_WATERMARK(event_time);

INSERT INTO store_sales
SELECT window_start, window_end, store_id,
       COUNT(*) AS orders,
       SUM(total) AS sales
FROM TUMBLE(
  (SELECT * FROM orders
   WHERE CURRENT_WATERMARK(event_time) IS NULL
      OR event_time > CURRENT_WATERMARK(event_time)),
  DESCRIPTOR(event_time),
  INTERVAL '1' MINUTE)
GROUP BY window_start, window_end, store_id;

END;
