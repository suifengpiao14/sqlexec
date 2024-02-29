create database `xyxz_manage_db` CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci;;

CREATE TABLE `t_xyxz_uniform_config` (
  `Fid` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `Fapp` varchar(100) NOT NULL DEFAULT '' COMMENT '程序',
  `Fkey` varchar(100) NOT NULL COMMENT '键',
  `Fvalue` varchar(10000) NOT NULL DEFAULT '' COMMENT '值',
  `Fstatus` char(1) NOT NULL DEFAULT '1' COMMENT '状态：0 废弃;1 使用',
  `Fauto_create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '自动创建时间',
  `Fauto_update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '自动更新时间',
  `Fadmin` varchar(100) NOT NULL DEFAULT '' COMMENT '管理员',
  `Fremark` varchar(1000) NOT NULL DEFAULT '' COMMENT '备注',
  PRIMARY KEY (`Fid`),
  UNIQUE KEY `Fapp` (`Fapp`,`Fkey`)
) ENGINE=InnoDB AUTO_INCREMENT=18 DEFAULT CHARSET=utf8 COMMENT='闲鱼小站杂七杂八配置表';

CREATE TABLE `t_xyxz_xy_cancel_remark_map` (
  `Fid` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT '自增ID',
  `Fhsb_remark` varchar(512) NOT NULL DEFAULT '' COMMENT '回收宝取消备注',
  `Fxy_remark` varchar(512) NOT NULL DEFAULT '' COMMENT '闲鱼取消备注',
  `Fpop_up_window` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否弹框自定义 0-否 1-是',
  `Fstatus` tinyint(4) NOT NULL DEFAULT '1' COMMENT '状态 0-无效 1-有效',
  `Fneed_pic` tinyint(4) NOT NULL DEFAULT '0' COMMENT '状态 0-不需要 1-需要',
  `Fcan_relation_old_order_id` tinyint(4) NOT NULL DEFAULT '0' COMMENT '是否需要关联旧订单 0-不需要 1-需要',
  `Fauto_create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `Fauto_update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
  PRIMARY KEY (`Fid`)
) ENGINE=InnoDB AUTO_INCREMENT=33 DEFAULT CHARSET=utf8 COMMENT='闲鱼订单取消备注映射表';