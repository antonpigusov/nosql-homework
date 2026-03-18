-- TODO: Implementation

CREATE KEYSPACE solution
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

-- Я использую post_id чтобы посты, которые были созданы в одно время хранились, ибо если не указать post_id, то
-- посты, созданные в одинаковое время, не будут дублироваться, а просто перезапишутся данные
CREATE TABLE posts_user_id (
    post_id int,
    user_id int,
    topic_id int,
    content text,
    username text,
    created_at timestamp,
    PRIMARY KEY (user_id, created_at, post_id)
) WITH CLUSTERING ORDER BY (created_at DESC);


CREATE TABLE posts_topic_id (
    topic_id int,
    user_id int,
    post_id int,
    username text,
    created_at timestamp,
    post_date date, 
    PRIMARY KEY ((topic_id, post_date), created_at, post_id)
) WITH CLUSTERING ORDER BY (created_at DESC);


INSERT INTO posts_user_id (post_id, user_id, topic_id, content, username, created_at)
VALUES (101, 1, 1, 'content1', 'name1', '2023-12-15 10:30:00');
INSERT INTO posts_topic_id (topic_id, user_id, post_id, username, created_at, post_date)
VALUES (1, 1, 101, 'name1', '2023-12-15 10:30:00', '2023-12-15');

INSERT INTO posts_user_id (post_id, user_id, topic_id, content, username, created_at)
VALUES (102, 1, 1, 'content2', 'name1', '2024-01-15 14:30:00');
INSERT INTO posts_topic_id (topic_id, user_id, post_id, username, created_at, post_date)
VALUES (1, 1, 102, 'name1', '2024-01-15 14:30:00', '2024-01-15');

INSERT INTO posts_user_id (post_id, user_id, topic_id, content, username, created_at)
VALUES (103, 1, 2, 'content3', 'name1', '2024-01-10 09:15:00');
INSERT INTO posts_topic_id (topic_id, user_id, post_id, username, created_at, post_date)
VALUES (2, 1, 103, 'name1', '2024-01-10 09:15:00', '2024-01-10');

INSERT INTO posts_user_id (post_id, user_id, topic_id, content, username, created_at)
VALUES (201, 2, 1, 'content4', 'name2', '2024-01-15 09:20:00');
INSERT INTO posts_topic_id (topic_id, user_id, post_id, username, created_at, post_date)
VALUES (1, 2, 201, 'name2', '2024-01-15 09:20:00', '2024-01-15');

INSERT INTO posts_user_id (post_id, user_id, topic_id, content, username, created_at)
VALUES (202, 2, 1, 'content5', 'name2', '2024-01-15 18:45:00');
INSERT INTO posts_topic_id (topic_id, user_id, post_id, username, created_at, post_date)
VALUES (1, 2, 202, 'name2', '2024-01-15 18:45:00', '2024-01-15');

INSERT INTO posts_user_id (post_id, user_id, topic_id, content, username, created_at)
VALUES (301, 3, 3, 'content6', 'name3', '2024-01-14 08:00:00');
INSERT INTO posts_topic_id (topic_id, user_id, post_id, username, created_at, post_date)
VALUES (3, 3, 301, 'name3', '2024-01-14 08:00:00', '2024-01-14');

INSERT INTO posts_user_id (post_id, user_id, topic_id, content, username, created_at)
VALUES (401, 4, 1, 'content7', 'name4', '2024-01-15 12:10:00');
INSERT INTO posts_topic_id (topic_id, user_id, post_id, username, created_at, post_date)
VALUES (1, 4, 401, 'name4', '2024-01-15 12:10:00', '2024-01-15');

INSERT INTO posts_user_id (post_id, user_id, topic_id, content, username, created_at)
VALUES (104, 1, 1, 'content8', 'name1', '2024-01-05 14:00:00');
INSERT INTO posts_topic_id (topic_id, user_id, post_id, username, created_at, post_date)
VALUES (1, 1, 104, 'name1', '2024-01-05 14:00:00', '2024-01-05');


SELECT post_id, content, created_at FROM posts_user_id WHERE user_id = 1;

SELECT post_id, content, created_at, username FROM posts_user_id WHERE user_id = 1 LIMIT 1;

SELECT post_id, content, created_at FROM posts_user_id WHERE user_id = 1 AND created_at > '2024-01-01';

SELECT user_id, username, post_id, created_at FROM posts_topic_id WHERE topic_id = 1 AND post_date = '2024-01-01';
