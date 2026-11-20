-- When does Flink add a SinkUpsertMaterializer? When the query's upsert key differs from
-- the sink's primary key. Here the aggregate is keyed by (store_id, channel), but the sink
-- by store_id alone.
--
-- Without an ON CONFLICT clause, Flink 2.3 rejects this INSERT: "The query has an upsert key
-- that differs from the primary key of the sink table ... Please specify an ON CONFLICT
-- clause". With DO DEDUPLICATE, DO ERROR or DO NOTHING it plans the sink with
-- upsertMaterialize=[true], and GroupAggregate starts emitting UPDATE_BEFORE.

CREATE TABLE orders (
  order_id BIGINT, store_id INT, pizza_id INT, channel STRING, total DECIMAL(10, 2),
  delivery_address STRING, event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
) WITH ('connector' = 'kafka', 'topic' = 'orders', 'properties.bootstrap.servers' = 'localhost:9092', 'scan.startup.mode' = 'earliest-offset', 'format' = 'json');
CREATE TABLE store_totals (
  store_id INT, orders BIGINT, sales DECIMAL(38, 2), PRIMARY KEY (store_id) NOT ENFORCED
) WITH ('connector' = 'upsert-kafka', 'topic' = 'store_totals', 'properties.bootstrap.servers' = 'localhost:9092', 'key.format' = 'json', 'value.format' = 'json');
EXPLAIN CHANGELOG_MODE
INSERT INTO store_totals
SELECT store_id, COUNT(*) AS orders, SUM(total) AS sales
FROM orders
GROUP BY store_id, channel
ON CONFLICT DO DEDUPLICATE;
