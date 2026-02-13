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
