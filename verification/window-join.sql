-- Pattern: window join (FULL OUTER, of two window aggregates)

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

CREATE TABLE kitchen_tickets (
  order_id      BIGINT,
  store_id      INT,
  prep_minutes  INT,
  event_time    TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH (
  'connector' = 'kafka', 'topic' = 'kitchen_tickets',
  'properties.bootstrap.servers' = 'localhost:9092',
  'scan.startup.mode' = 'earliest-offset', 'format' = 'json'
);

CREATE TABLE kitchen_load (
  store_id      INT,
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  placed        BIGINT,
  cooked        BIGINT
) WITH (
  'connector' = 'kafka', 'topic' = 'kitchen_load',
  'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json'
);

EXPLAIN CHANGELOG_MODE
INSERT INTO kitchen_load
SELECT COALESCE(o.store_id, k.store_id) AS store_id,
       COALESCE(o.window_start, k.window_start) AS window_start,
       COALESCE(o.window_end, k.window_end) AS window_end,
       o.placed, k.cooked
FROM (
  SELECT store_id, window_start, window_end, COUNT(*) AS placed
  FROM TUMBLE(TABLE orders, DESCRIPTOR(event_time), INTERVAL '15' MINUTES)
  GROUP BY store_id, window_start, window_end) o
FULL OUTER JOIN (
  SELECT store_id, window_start, window_end, COUNT(*) AS cooked
  FROM TUMBLE(TABLE kitchen_tickets, DESCRIPTOR(event_time), INTERVAL '15' MINUTES)
  GROUP BY store_id, window_start, window_end) k
ON o.store_id = k.store_id
AND o.window_start = k.window_start AND o.window_end = k.window_end;
