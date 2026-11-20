-- Pattern: non-windowed aggregation (three variants)

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

-- Variant 1: running total into an upsert sink

CREATE TABLE store_totals (
  store_id  INT,
  orders    BIGINT,
  sales     DECIMAL(38, 2),
  PRIMARY KEY (store_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka', 'topic' = 'store_totals',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'json', 'value.format' = 'json'
);

EXPLAIN CHANGELOG_MODE
INSERT INTO store_totals
SELECT store_id,
       COUNT(*) AS orders,
       SUM(total) AS sales
FROM orders
GROUP BY store_id;

-- Variant 2: the same running total into a changelog (retract) sink

CREATE TABLE store_totals_changelog (
  store_id  INT,
  orders    BIGINT,
  sales     DECIMAL(38, 2)
) WITH (
  'connector' = 'kafka', 'topic' = 'store_totals_changelog',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'debezium-json'
);

EXPLAIN CHANGELOG_MODE
INSERT INTO store_totals_changelog
SELECT store_id,
       COUNT(*) AS orders,
       SUM(total) AS sales
FROM orders
GROUP BY store_id;

-- Variant 3: aggregating an updating stream

CREATE TABLE order_status (
  order_id  BIGINT,
  store_id  INT,
  status    STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka', 'topic' = 'order_status',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'json', 'value.format' = 'json'
);

CREATE TABLE status_counts (
  status  STRING,
  orders  BIGINT,
  PRIMARY KEY (status) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka', 'topic' = 'status_counts',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'json', 'value.format' = 'json'
);

EXPLAIN CHANGELOG_MODE
INSERT INTO status_counts
SELECT status,
       COUNT(*) AS orders
FROM order_status
GROUP BY status;
