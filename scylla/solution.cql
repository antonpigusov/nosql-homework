CREATE KEYSPACE IF NOT EXISTS messenger
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE messenger;


CREATE TABLE IF NOT EXISTS posts_by_user
(
    user_id    int,
    created_at timestamp,
    post_id    int,
    username   text,
    topic_id   int,
    content    text,
    PRIMARY KEY (user_id, created_at, post_id)
) WITH CLUSTERING ORDER BY (created_at DESC, post_id ASC);


CREATE TABLE IF NOT EXISTS posts_by_topic
(
    topic_id   int,
    created_at timestamp,
    post_id    int,
    user_id    int,
    username   text,
    PRIMARY KEY (topic_id, created_at, post_id)
) WITH CLUSTERING ORDER BY (created_at DESC, post_id ASC);


-- Запрос, который бы доставал посты заданного пользователя, упорядоченные по времени создания поста (сначала новые).
-- Должен возвращать: post_id, content, дату создания поста.
SELECT post_id, content, created_at FROM posts_by_user WHERE user_id = 2;

-- Запрос, который бы доставал последний пост заданного пользователя.
-- Должен возвращать: post_id, content, дату создания поста, username.
SELECT post_id, content, created_at, username FROM posts_by_user WHERE user_id = 2 LIMIT 1;

-- Запрос, который бы доставал посты заданного пользователя после заданной даты.
-- Должен возвращать: post_id, content, дату создания поста.
SELECT post_id, content, created_at FROM posts_by_user
WHERE user_id = 2 AND created_at > '2024-01-17 18:46:00';

-- Запрос, который бы доставал пользователей, сделавших пост в заданный день по заданной теме, упорядоченные по времени (сначала новые).
-- Должен возвращать: user_id, username, post_id, время создания поста.
SELECT user_id, username, post_id, created_at
FROM posts_by_topic
WHERE topic_id = 4
  AND created_at >= '2024-01-17 00:00:00'
  AND created_at <= '2024-01-17 23:59:59';