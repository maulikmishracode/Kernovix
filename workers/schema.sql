CREATE DATABASE IF NOT EXISTS kernovix;
USE kernovix;

CREATE TABLE IF NOT EXISTS processed_events (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_type    VARCHAR(100),
    source_id     VARCHAR(255),
    happened_at   DATETIME,
    data          JSON,
    redis_id      VARCHAR(100) UNIQUE,
    saved_at      DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dead_letter_events (
    id          BIGINT AUTO_INCREMENT PRIMARY KEY,
    redis_id    VARCHAR(100),
    reason      TEXT,
    raw_data    TEXT,
    failed_at   DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    events_good   INT DEFAULT 0,
    events_bad    INT DEFAULT 0,
    ran_at        DATETIME DEFAULT CURRENT_TIMESTAMP