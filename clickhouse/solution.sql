-- Решение заданий по ClickHouse

-- 1. Создание таблицы
-- TODO: скопируйте и доработайте CREATE TABLE из schema.sql


-- 2. Загрузка данных из CSV
-- Подсказка: можно использовать clickhouse-client с параметром --query
-- Пример команды (выполняется в терминале):
-- cat server_logs.csv | clickhouse-client --query="INSERT INTO server_logs FORMAT CSVWithNames"


-- 3. Запрос: Топ-5 самых медленных endpoint'ов (по среднему времени ответа)
-- TODO: напишите SELECT запрос


-- 4. Запрос: Количество запросов по часам за весь период в логах
-- TODO: напишите SELECT запрос с использованием функции toHour() или formatDateTime()


-- 5. Запрос: Процент ошибок (status_code >= 400) для каждого endpoint'а
-- TODO: напишите SELECT запрос с вычислением процента ошибок
CREATE TABLE IF NOT EXISTS server_logs
(
    timestamp DateTime,
    user_id UInt32,
    endpoint String,
    response_time_ms UInt32,
    status_code UInt16
)
    ENGINE = MergeTree()
        ORDER BY (timestamp, endpoint);
-- Топ-5 самых медленных endpoint'ов (по среднему времени ответа response_time_ms)
SELECT endpoint, avg(response_time_ms) as avg_repsonse
FROM server_logs
GROUP BY endpoint
ORDER BY endpoint DESC
LIMIT 5;

-- Количество запросов по часам за весь период в логах
SELECT
    toStartOfHour(timestamp) AS hour,
    count() AS request_count
FROM server_logs
GROUP BY hour
ORDER BY hour ASC;

-- Процент ошибок (status_code >= 400) для каждого endpoint'а
SELECT
    endpoint,
    round(countIf(status_code >= 400) * 100.0 / count(), 3) AS error_endpoint
FROM server_logs
GROUP BY endpoint
ORDER BY error_endpoint DESC

