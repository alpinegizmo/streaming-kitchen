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

-- Updating input: each store's first order that hasn't been cancelled, from order_status, an
-- upsert-kafka table. The planner keeps the Rank (RetractStrategy) instead of a Deduplicate.

CREATE TABLE order_status (
  order_id    BIGINT,
  store_id    INT,
  status      STRING,
  total       DECIMAL(10, 2),
  event_time  TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED,
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
  'connector' = 'upsert-kafka', 'topic' = 'order_status',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'json', 'value.format' = 'json'
);

CREATE TABLE first_orders (
  store_id    INT,
  order_id    BIGINT,
  total       DECIMAL(10, 2),
  event_time  TIMESTAMP(3),
  PRIMARY KEY (store_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka', 'topic' = 'first_orders',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'json', 'value.format' = 'json'
);

EXPLAIN CHANGELOG_MODE
INSERT INTO first_orders
SELECT store_id, order_id, total, event_time
FROM (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY store_id
                       ORDER BY event_time ASC) AS row_num
  FROM order_status
  WHERE status <> 'cancelled')
WHERE row_num = 1;
