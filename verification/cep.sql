-- Pattern: complex event processing (MATCH_RECOGNIZE)

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

CREATE TABLE slow_kitchens (
  store_id       INT,
  first_minutes  INT,
  last_minutes   INT,
  detected_at    TIMESTAMP(3)
) WITH (
  'connector' = 'kafka', 'topic' = 'slow_kitchens',
  'properties.bootstrap.servers' = 'localhost:9092', 'format' = 'json'
);

EXPLAIN CHANGELOG_MODE
INSERT INTO slow_kitchens
SELECT *
FROM kitchen_tickets MATCH_RECOGNIZE (
  PARTITION BY store_id
  ORDER BY event_time
  MEASURES A.prep_minutes AS first_minutes,
           C.prep_minutes AS last_minutes,
           MATCH_ROWTIME() AS detected_at
  ONE ROW PER MATCH
  AFTER MATCH SKIP PAST LAST ROW
  PATTERN (A B C)
  DEFINE
    B AS B.prep_minutes > A.prep_minutes,
    C AS C.prep_minutes > B.prep_minutes
);
