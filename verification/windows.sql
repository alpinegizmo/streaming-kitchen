-- Pattern: windows (tumble, hop, cumulate, session)

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

EXPLAIN CHANGELOG_MODE
INSERT INTO store_sales
SELECT window_start, window_end, store_id,
       COUNT(*) AS orders,
       SUM(total) AS sales
FROM TUMBLE(
  TABLE orders,
  DESCRIPTOR(event_time),
  INTERVAL '5' MINUTES)
GROUP BY window_start, window_end, store_id;

-- Hop: the last 15 minutes, every 5 minutes

EXPLAIN CHANGELOG_MODE
INSERT INTO store_sales
SELECT window_start, window_end, store_id,
       COUNT(*) AS orders,
       SUM(total) AS sales
FROM HOP(
  TABLE orders,
  DESCRIPTOR(event_time),
  INTERVAL '5' MINUTES,
  INTERVAL '15' MINUTES)
GROUP BY window_start, window_end, store_id;

-- Cumulate: so far this hour, every 15 minutes

EXPLAIN CHANGELOG_MODE
INSERT INTO store_sales
SELECT window_start, window_end, store_id,
       COUNT(*) AS orders,
       SUM(total) AS sales
FROM CUMULATE(
  TABLE orders,
  DESCRIPTOR(event_time),
  INTERVAL '15' MINUTES,
  INTERVAL '1' HOUR)
GROUP BY window_start, window_end, store_id;

-- Session: bursts of orders, per store, separated by 10 quiet minutes

EXPLAIN CHANGELOG_MODE
INSERT INTO store_sales
SELECT window_start, window_end, store_id,
       COUNT(*) AS orders,
       SUM(total) AS sales
FROM SESSION(
  TABLE orders PARTITION BY store_id,
  DESCRIPTOR(event_time),
  INTERVAL '10' MINUTES)
GROUP BY window_start, window_end, store_id;
