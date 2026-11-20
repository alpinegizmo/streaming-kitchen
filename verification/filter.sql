-- Pattern: filtering and projection (both variants)

CREATE TABLE orders (
  order_id          BIGINT,
  store_id          INT,
  pizza_id          INT,
  channel           STRING,
  total             DECIMAL(10,2),
  delivery_address  STRING,
  event_time        TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
  'connector' = 'kafka', 'topic' = 'orders',
  'properties.bootstrap.servers' = 'localhost:9092',
  'scan.startup.mode' = 'earliest-offset', 'format' = 'json'
);

CREATE TABLE delivery_orders (
  order_id        BIGINT,
  store_id        INT,
  total_with_tax  DECIMAL(10,2),
  event_time      TIMESTAMP(3)
) WITH (
  'connector' = 'kafka', 'topic' = 'delivery_orders',
  'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json'
);

EXPLAIN CHANGELOG_MODE
INSERT INTO delivery_orders
SELECT order_id,
       store_id,
       total * 1.08 AS total_with_tax,
       event_time
FROM orders
WHERE channel = 'delivery';

CREATE TABLE order_status (
  order_id    BIGINT,
  store_id    INT,
  status      STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka', 'topic' = 'order_status',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'json', 'value.format' = 'json'
);

CREATE TABLE open_orders (
  order_id    BIGINT,
  store_id    INT,
  status      STRING,
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka', 'topic' = 'open_orders',
  'properties.bootstrap.servers' = 'localhost:9092',
  'key.format' = 'json', 'value.format' = 'json'
);

EXPLAIN CHANGELOG_MODE
INSERT INTO open_orders
SELECT order_id, store_id, status
FROM order_status
WHERE status <> 'cancelled';
