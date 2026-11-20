-- Pattern: enrichment (lookup join and event-time temporal join)

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

-- Lookup join. The page describes routes as a maps API behind a lookup connector. The plan is
-- the same for any connector that implements LookupTableSource; JDBC is used here because it
-- ships with Apache Flink.

CREATE TABLE routes (
  store_id       INT,
  address        STRING,
  drive_minutes  INT
) WITH (
  'connector' = 'jdbc', 'url' = 'jdbc:postgresql://localhost:5432/maps', 'table-name' = 'routes',
  'lookup.cache' = 'PARTIAL', 'lookup.partial-cache.max-rows' = '10000',
  'lookup.partial-cache.expire-after-write' = '1 h'
);

CREATE TABLE delivery_times (
  order_id          BIGINT,
  delivery_address  STRING,
  drive_minutes     INT
) WITH (
  'connector' = 'kafka', 'topic' = 'delivery_times',
  'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json'
);

EXPLAIN CHANGELOG_MODE
INSERT INTO delivery_times
SELECT o.order_id, o.delivery_address, r.drive_minutes
FROM orders AS o
JOIN routes FOR SYSTEM_TIME AS OF o.proc_time AS r
  ON o.store_id = r.store_id
 AND o.delivery_address = r.address;

-- Event-time temporal join

CREATE TABLE pizza_costs (
  pizza_id    INT,
  food_cost   DECIMAL(10, 2),
  valid_from  TIMESTAMP(3),
  PRIMARY KEY (pizza_id) NOT ENFORCED,
  WATERMARK FOR valid_from AS valid_from - INTERVAL '1' SECOND
) WITH (
  'connector' = 'upsert-kafka', 'topic' = 'pizza_costs',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'json', 'value.format' = 'json'
);

CREATE TABLE order_costs (
  order_id   BIGINT,
  pizza_id   INT,
  total      DECIMAL(10, 2),
  food_cost  DECIMAL(10, 2)
) WITH (
  'connector' = 'kafka', 'topic' = 'order_costs',
  'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json'
);

EXPLAIN CHANGELOG_MODE
INSERT INTO order_costs
SELECT o.order_id, o.pizza_id, o.total, c.food_cost
FROM orders AS o
JOIN pizza_costs FOR SYSTEM_TIME AS OF o.event_time AS c
  ON o.pizza_id = c.pizza_id;
