-- Pattern: enrichment with LATERAL SNAPSHOT (FLIP-579). Not in a released Flink yet:
-- run with FLINK_VERSION=2.4-SNAPSHOT.

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
JOIN LATERAL SNAPSHOT(
       input   => TABLE pizza_costs,
       on_time => DESCRIPTOR(valid_from)) AS c
  ON o.pizza_id = c.pizza_id;
