CREATE TABLE users
(
    user_id  INTEGER PRIMARY KEY,
    username VARCHAR(50),
    email    VARCHAR(100)
);

CREATE TABLE posts
(
    post_id    INTEGER PRIMARY KEY,
    user_id    INTEGER REFERENCES users (user_id),
    topic_id   INTEGER,
    content    TEXT,
    created_at TIMESTAMP
);
