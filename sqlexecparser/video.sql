
-- 创建数据库
create database `video` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;

-- 创建图片存储记录表,url 必须唯一
CREATE TABLE `image` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT comment '自增ID',
  `url` varchar(1024) DEFAULT '' comment '绝对访问地址' ,
  `type` varchar(50) not null default 'thumb' comment '类型:thumb-封面图,detail-详情图',
  `user_id` bigint(20) unsigned DEFAULT 0 comment '创建者ID',
  `video_id` bigint(20) unsigned DEFAULT 0 comment '视频ID',
  `ext` varchar(20)  not null DEFAULT "jpg" comment '图片格式:(jpg-jpg,png-png,webp-webp)',
  `created_at` datetime not null DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime not null DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `deleted_at` datetime  DEFAULT null COMMENT '删除时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_url` (`url`) USING BTREE,
  KEY `idx_video_id` (`video_id`,`deleted_at`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;

-- 创建分类表
CREATE TABLE `categorize` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT comment '自增ID',
   `parent_id` bigint(20) unsigned not null DEFAULT 0 comment '父类分类ID',
   `path` varchar(1024)  not null DEFAULT '/' comment '分类路径',
  `is_leaf` varchar(10)  not null DEFAULT 'yes' comment '是否为叶子节点(yes-是,no-否)',
 `is_show` varchar(10)  not null DEFAULT 'yes' comment '是否为展示(yes-是,no-否)',
  `name` varchar(128) DEFAULT '' comment '分类名称' ,
  `dimension` varchar(128) DEFAULT 'subject' comment '分类维度(subject-学科,grade-年级,topic-专题,publisher-出版社)' ,
  `created_at` datetime not null DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime not null DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `deleted_at` datetime  DEFAULT null COMMENT '删除时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;

insert into `categorize` (is_leaf,is_show,name,dimension)values
('no','yes','语文','subject'),
('no','yes','数学','subject'),
('no','yes','英语','subject'),
('no','yes','科学','subject'),
('no','yes','道法','subject'),
('no','yes','体育','subject');
insert into `categorize` (is_leaf,is_show,name,dimension)values
('no','yes','胎教','grade'),
('no','yes','0-3岁','grade'),
('no','yes','一年级','grade'),
('no','yes','二年级','grade'),
('no','yes','三年级','grade'),
('no','yes','四年级','grade'),
('no','yes','五年级','grade'),
('no','yes','六年级','grade'),
('no','yes','七年级','grade'),
('no','yes','八年级','grade'),
('no','yes','九年级','grade'),
('no','yes','高一','grade'),
('no','yes','高二','grade'),
('no','yes','高三','grade');
insert into `categorize` (is_leaf,is_show,name,dimension)values
('no','yes','拼音','topic'),
('no','yes','古诗','topic'),
('no','yes','自然拼读','topic'),
('no','yes','口算','topic'),
('no','yes','弟子规','topic'),
('no','yes','乘法表','topic');
insert into `categorize` (is_leaf,is_show,name,dimension)values
('no','yes','苏教版','publisher'),
('no','yes','人教版','publisher'),
('no','yes','北师大版','publisher');

-- 创建分类视频关联表
CREATE TABLE `categorize_video` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT comment '自增ID',
  `categorize_id` bigint(20) unsigned not null DEFAULT 0 comment '分类ID',
  `video_id` bigint(20) unsigned DEFAULT 0 comment '视频ID',
 `categorize_dimension` varchar(128) DEFAULT 'subject' comment '分类维度categorize.dimension' ,
 PRIMARY KEY (`id`),
  UNIQUE KEY `uk_categorize_id_video_id` (`categorize_id`,`video_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;

-- 创建视频存储记录表,url 必须唯一
CREATE TABLE `video` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT comment '自增ID',
  `name` varchar(256) not null DEFAULT '' comment '视频名称' ,
   `summary` varchar(256) not null DEFAULT '' comment '简介' ,
   `subject` varchar(20) not null DEFAULT '' comment '科目:(chinese-语文,maths-数学,english-英语,science-科学,dow-道法,physical-体育)',
  `url` varchar(1024) not null DEFAULT '' comment '绝对访问地址' ,
  `user_id` bigint(20) unsigned not null DEFAULT 0 comment '创建者ID',
  `ext` varchar(20) not null DEFAULT "mp4" comment '视频格式( mp4-mp4,flv-flv,mp3-mp3)',
  `description` varchar(10240) not null default '' comment '视频介绍',
  `created_at` datetime not null DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime not null DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `deleted_at` datetime  DEFAULT null COMMENT '删除时间',
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_url` (`url`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;


-- 创建检索表
CREATE TABLE `index` (
  `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT comment '自增ID',
   `categorize_ids` varchar(20)  not null DEFAULT '' comment '分类ID',
  `name` varchar(256) not null DEFAULT '' comment 'ref:video.name' ,
  `summary` varchar(256) not null DEFAULT '' comment 'ref:video.summary' ,
  `categorize_names` varchar(128) not null DEFAULT '' comment '分类名称,多个逗号分隔' ,
  `tag` varchar(1024) not null DEFAULT '' comment '标签',
  `hits` bigint(20) not null DEFAULT 0 comment '查看次数',
    `created_at` datetime not null DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `updated_at` datetime not null DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `deleted_at` datetime  DEFAULT null COMMENT '删除时间',
  PRIMARY KEY (`id`),
  KEY `idx_search_deleted_at` (`deleted_at`)
) ENGINE=InnoDB AUTO_INCREMENT=249 DEFAULT CHARSET=utf8mb4;
