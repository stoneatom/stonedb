CREATE TABLE IF NOT EXISTS $table_name (
  `seller_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '卖家ID',
  `feed_id` bigint(20) NOT NULL DEFAULT '0' COMMENT '主评ID',
  `keywords` varchar(1024) COLLATE utf8_unicode_ci DEFAULT NULL COMMENT '订阅词',
  `keywords_md5` char(32) COLLATE utf8_unicode_ci NOT NULL DEFAULT '' COMMENT '订阅词md5',
  `gmt_create` datetime DEFAULT NULL COMMENT '创建时间',
  PRIMARY KEY (`seller_id`,`feed_id`,`keywords_md5`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_unicode_ci
