-- Pattern: OVER aggregation (event-time RANGE window)

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

CREATE TABLE orders_with_recent_sales (
  order_id       BIGINT,
  store_id       INT,
  total          DECIMAL(10, 2),
  event_time     TIMESTAMP(3),
  recent_orders  BIGINT,
  recent_sales   DECIMAL(38, 2)
) WITH (
  'connector' = 'kafka', 'topic' = 'orders_with_recent_sales',
  'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json'
);

EXPLAIN CHANGELOG_MODE
INSERT INTO orders_with_recent_sales
SELECT order_id, store_id, total, event_time,
       COUNT(*) OVER w AS recent_orders,
       SUM(total) OVER w AS recent_sales
FROM orders
WINDOW w AS (
  PARTITION BY store_id
  ORDER BY event_time
  RANGE BETWEEN INTERVAL '5' MINUTE PRECEDING AND CURRENT ROW);
