-- Решение заданий по ClickHouse

-- 1. Создание таблицы
-- TODO: скопируйте и доработайте CREATE TABLE из schema.sql
CREATE TABLE IF NOT EXISTS server_logs
(
    timestamp DateTime,
    user_id UInt64,
    endpoint FixedString(25),
    response_time_ms UInt64,
    status_code UInt16
) ENGINE = MergeTree()
ORDER BY (endpoint, timestamp); -- TODO: выберите подходящий порядок сортировки

-- 2. Загрузка данных из CSV
-- Подсказка: можно использовать clickhouse-client с параметром --query
-- Пример команды (выполняется в терминале):
-- cat server_logs.csv | clickhouse-client --query="INSERT INTO server_logs FORMAT CSVWithNames"


-- 3. Запрос: Топ-5 самых медленных endpoint'ов (по среднему времени ответа)
-- TODO: напишите SELECT запрос
SELECT endpoint, avg(response_time_ms) AS avg_response_time
FROM server_logs GROUP BY endpoint ORDER BY avg_response_time LIMIT 5;

-- 4. Запрос: Количество запросов по часам за весь период в логах
-- TODO: напишите SELECT запрос с использованием функции toHour() или formatDateTime()
SELECT toHour(timestamp) AS hour, count() AS requests FROM server_logs
GROUP BY toHour(timestamp)
ORDER BY hour;

-- 5. Запрос: Процент ошибок (status_code >= 400) для каждого endpoint'а
-- TODO: напишите SELECT запрос с вычислением процента ошибок
SELECT endpoint, count() AS total_requests, countIf(status_code >= 400) AS errors, round(100 * errors / total_requests, 2) AS error_percent
FROM server_logs
GROUP BY endpoint
ORDER BY error_percent DESC;
