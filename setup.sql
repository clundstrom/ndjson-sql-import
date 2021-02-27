DROP DATABASE IF EXISTS reddit;
CREATE DATABASE IF NOT EXISTS reddit;
USE reddit;

CREATE TABLE IF NOT EXISTS `subreddit_constraints` (
    `subreddit_id` varchar(10) NOT NULL,
    `subreddit` tinytext NOT NULL,
    PRIMARY KEY (`subreddit_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `posts_constraints` (
    `id` varchar(10) NOT NULL,
    `parent_id` varchar(50) NOT NULL,
    `link_id` varchar(50) NOT NULL,
    `name` tinytext NOT NULL,
    `author` varchar(25) NOT NULL,
    `body` longtext NOT NULL,
    `fk_subreddit_id` varchar(10) NOT NULL,
    `score` int(20) NOT NULL,
    `created_utc` int(20) NOT NULL,
    PRIMARY KEY (`id`),
    INDEX (author),
    FOREIGN KEY (`fk_subreddit_id`)
        REFERENCES `subreddit_constraints`(`subreddit_id`)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

/*********************
NO CONSTRAINTS TEST
**********************/

CREATE TABLE IF NOT EXISTS `subreddit_no_constraints` (
    `subreddit_id` varchar(10),
    `subreddit` tinytext
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `posts_no_constraints` (
    `id` varchar(10),
    `parent_id` varchar(50),
    `link_id` varchar(50),
    `name` tinytext,
    `author` tinytext,
    `body` longtext ,
    `fk_subreddit_id` varchar(10),
    `score` int(20),
    `created_utc` int(20)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;