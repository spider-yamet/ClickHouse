SET enable_analyzer = 1;

DROP TABLE IF EXISTS with_fill_function_column_03810;

CREATE TABLE with_fill_function_column_03810
(
    ts DateTime64(9, 'UTC'),
    val1 Float64
)
ENGINE = MergeTree
ORDER BY ts;

INSERT INTO with_fill_function_column_03810 VALUES
    ('2026-02-25 10:00:00', 100),
    ('2026-02-25 10:00:10', 200);

SELECT
    count() AS rows_count,
    countIf(timestamp = 0) AS zero_timestamp_count
FROM
(
    WITH toStartOfInterval(ts, INTERVAL 1 SECOND) AS timewindow
    SELECT
        toUnixTimestamp(timewindow) AS timestamp
    FROM with_fill_function_column_03810
    GROUP BY timewindow
    ORDER BY timewindow WITH FILL STEP INTERVAL 1 SECOND
);

DROP TABLE IF EXISTS with_fill_function_column_03810;
