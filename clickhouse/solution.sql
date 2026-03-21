-- Решение заданий по ClickHouse

-- 1. Создание таблицы
CREATE TABLE IF NOT EXISTS server_logs
(
    timestamp DATETIME,
    user_id UInt32,
    endpoint String,
    response_time_ms UInt32,
    status_code UInt16
) ENGINE = MergeTree()
    ORDER BY (endpoint, timestamp);


-- 2. Загрузка данных из CSV
-- Подсказка: можно использовать clickhouse-client с параметром --query
-- Пример команды (выполняется в терминале):
-- cat server_logs.csv | clickhouse-client --query="INSERT INTO server_logs FORMAT CSVWithNames"


-- 3. Топ-5 самых медленных endpoint'ов (по среднему времени ответа)
SELECT endpoint, avg(response_time_ms) AS avg_response_time
FROM server_logs
    GROUP BY endpoint
    ORDER BY avg_response_time DESC
LIMIT 5;


-- 4. Количество запросов по часам за весь период в логах
SELECT
    toHour(timestamp) AS hour,
    count() AS request_count
FROM server_logs
    GROUP BY hour
    ORDER BY hour;

-- 5. Процент ошибок (status_code >= 400) для каждого endpoint'а
SELECT
    endpoint,
    count() AS total_requests,
    countIf(status_code >= 400) AS error_count,
    round(countIf(status_code >= 400) * 100.0 / count(), 2) AS error_percentage
FROM server_logs
    GROUP BY endpoint
    ORDER BY error_percentage DESC;