-- Pattern: deduplication (processing time and event time)

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

-- Processing time: keep the first copy of each order to arrive

CREATE TABLE unique_orders (
  order_id    BIGINT,
  store_id    INT,
  total       DECIMAL(10, 2),
  event_time  TIMESTAMP(3)
) WITH (
  'connector' = 'kafka', 'topic' = 'unique_orders',
  'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json'
);

EXPLAIN CHANGELOG_MODE
INSERT INTO unique_orders
SELECT order_id, store_id, total, event_time
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY order_id
                       ORDER BY proc_time ASC) AS row_num
  FROM orders)
WHERE row_num = 1;

-- Event time: keep the earliest version of each order, by event time

CREATE TABLE earliest_orders (
  order_id    BIGINT,
  store_id    INT,
  total       DECIMAL(10, 2),
  event_time  TIMESTAMP(3)
) WITH (
  'connector' = 'kafka', 'topic' = 'earliest_orders',
  'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json'
);

EXPLAIN CHANGELOG_MODE
INSERT INTO earliest_orders
SELECT order_id, store_id, total, event_time
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY order_id
                       ORDER BY event_time ASC) AS row_num
  FROM orders)
WHERE row_num = 1;
