create database `order`;
use order

CREATE TABLE `event` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `biz_id` bigint(20) NOT NULL DEFAULT '0',
  `biz_data` varchar(1000) NOT NULL DEFAULT '',
  `status` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `biz_id` (`biz_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;