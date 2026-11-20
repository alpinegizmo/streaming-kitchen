-- Pattern: regular join (store leaderboard)

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

CREATE TABLE stores (
  store_id  INT,
  city      STRING,
  manager   STRING,
  PRIMARY KEY (store_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka', 'topic' = 'stores',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'json', 'value.format' = 'json'
);

CREATE TABLE store_leaderboard (
  store_id  INT,
  city      STRING,
  manager   STRING,
  orders    BIGINT,
  sales     DECIMAL(38, 2),
  PRIMARY KEY (store_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka', 'topic' = 'store_leaderboard',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'json', 'value.format' = 'json'
);

EXPLAIN CHANGELOG_MODE
INSERT INTO store_leaderboard
SELECT s.store_id, s.city, s.manager, t.orders, t.sales
FROM (
  SELECT store_id, COUNT(*) AS orders, SUM(total) AS sales
  FROM orders
  GROUP BY store_id
) AS t
JOIN stores AS s
  ON t.store_id = s.store_id;

-- The anti-pattern: join raw orders with stores first, then aggregate. Only its plan matters,
-- so it writes to a blackhole sink.
CREATE TABLE leaderboard_blackhole (
  store_id  INT,
  city      STRING,
  manager   STRING,
  orders    BIGINT,
  sales     DECIMAL(38, 2)
) WITH ('connector' = 'blackhole');

EXPLAIN CHANGELOG_MODE
INSERT INTO leaderboard_blackhole
SELECT s.store_id, s.city, s.manager, COUNT(*) AS orders, SUM(o.total) AS sales
FROM orders AS o
JOIN stores AS s
  ON o.store_id = s.store_id
GROUP BY s.store_id, s.city, s.manager;

-- The same anti-pattern, written to the real leaderboard table. Its upsert key
-- (store_id, city, manager) differs from the sink's primary key, so Flink 2.3 requires ON CONFLICT.
EXPLAIN CHANGELOG_MODE
INSERT INTO store_leaderboard
SELECT s.store_id, s.city, s.manager, COUNT(*) AS orders, SUM(o.total) AS sales
FROM orders AS o
JOIN stores AS s
  ON o.store_id = s.store_id
GROUP BY s.store_id, s.city, s.manager
ON CONFLICT DO DEDUPLICATE;
