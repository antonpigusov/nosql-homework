-- TODO: Implementation

CREATE KEYSPACE IF NOT EXISTS homework
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE homework;

CREATE TABLE IF NOT EXISTS posts_by_user (
    user_id int,
    post_id int,
    content text,
    created_at timestamp,
    username text,
    PRIMARY KEY (user_id, created_at, post_id)
) WITH CLUSTERING 
    ORDER BY (created_at DESC, post_id DESC);

CREATE TABLE IF NOT EXISTS posts_by_topic_date (
    topic_id int,
    post_date date,
    created_at timestamp,
    post_id int,
    user_id int,
    username text,
    PRIMARY KEY ((topic_id, post_date), created_at, post_id)
) WITH CLUSTERING 
    ORDER BY (created_at DESC, post_id DESC);

INSERT INTO posts_by_user (user_id, post_id, content, created_at, username)
    VALUES (10, 1001, 'Привет всем подписчики! Сегодня я тестирую базу данных.', '2026-03-20 08:15:00+0000', 'Dima');
INSERT INTO posts_by_user (user_id, post_id, content, created_at, username)
    VALUES (10, 1002, 'Моя база упала!', '2026-03-20 09:45:00+0000', 'Dima');
INSERT INTO posts_by_user (user_id, post_id, content, created_at, username)
    VALUES (11, 2001, 'Сегодня я ревьюю базу Димы', '2026-03-20 10:00:00+0000', 'Artur');
INSERT INTO posts_by_user (user_id, post_id, content, created_at, username)
    VALUES (12, 3001, 'У Димы крутая база, берем его!', '2026-03-19 18:30:00+0000', 'Ivan');

INSERT INTO posts_by_topic_date (topic_id, post_date, created_at, post_id, user_id, username)
    VALUES (101, '2026-03-20', '2026-03-20 09:45:00+0000', 1002, 10, 'Dima');
INSERT INTO posts_by_topic_date (topic_id, post_date, created_at, post_id, user_id, username)
    VALUES (102, '2026-03-20', '2026-03-20 10:00:00+0000', 2001, 11, 'Artur');
INSERT INTO posts_by_topic_date (topic_id, post_date, created_at, post_id, user_id, username)
    VALUES (103, '2026-03-19', '2026-03-19 18:30:00+0000', 3001, 12, 'Ivan');

SELECT post_id, content, created_at
    FROM posts_by_user
WHERE user_id = 10;

SELECT post_id, content, created_at, username
    FROM posts_by_user
WHERE user_id = 11
    LIMIT 1;

SELECT post_id, content, created_at
    FROM posts_by_user
WHERE user_id = 10 
    AND created_at > '2026-03-20 08:30:00+0000';

SELECT user_id, username, post_id, created_at
    FROM posts_by_topic_date
WHERE topic_id = 102
    AND post_date = '2026-03-20';