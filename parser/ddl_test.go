package parser_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/suifengpiao14/sqlexec/parser"
)

var createDDLStr = `CREATE TABLE ad.plan (
	id int(11) NOT NULL AUTO_INCREMENT COMMENT '主键',
	advertiser_id varchar(32) NOT NULL COMMENT '广告主',
	name varchar(32) NOT NULL COMMENT '名称',
	position varchar(32) NOT NULL COMMENT '位置编码',
	begin_at datetime DEFAULT null COMMENT '投放开始时间',
	end_at datetime DEFAULT null  COMMENT '投放结束时间',
	did int(11) DEFAULT 0 COMMENT '出价',
	landing_page varchar(100) NOT NULL COMMENT '落地页',
	created_at datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
	updated_at datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
	deleted_at datetime  DEFAULT null COMMENT '删除时间',
	PRIMARY KEY (id),
	index  ik_advertiser_id (advertiser_id) USING BTREE,
	index  ik_position (position) USING BTREE
  ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8  COMMENT='广告计划';
  
  CREATE TABLE ad.window (
	id  int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
	media_id varchar(32) NOT NULL COMMENT '媒体Id',
	position varchar(32) NOT NULL COMMENT '位置编码',
	name varchar(32) NOT NULL COMMENT '位置名称',
	remark varchar(255) NOT NULL COMMENT '广告位描述(建议记录位置、app名称等)',
	scheme varchar(2048) DEFAULT '' COMMENT '广告素材的格式规范',
	created_at datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
	updated_at datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
	deleted_at datetime  DEFAULT null COMMENT '删除时间',
	PRIMARY KEY (id),
	UNIQUE KEY uk_position (position,deleted_at) USING BTREE
  ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC COMMENT='广告位表';
  
  CREATE TABLE ad.creative (
	id int(11) NOT NULL AUTO_INCREMENT COMMENT '主键',
	plan_id varchar(32) NOT NULL COMMENT '广告计划Id',
	name varchar(32) NOT NULL COMMENT '名称',
	content varchar(2048) DEFAULT '' COMMENT '广告内容',
	created_at datetime DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
	updated_at datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
	deleted_at datetime  DEFAULT null COMMENT '删除时间',
	PRIMARY KEY (id),
	index  ik_plan_id (plan_id) USING BTREE
  ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8  COMMENT='广告物料';
`

func TestParseCreateDDL(t *testing.T) {
	table, err := parser.ParseCreateDDL(createDDLStr)
	require.NoError(t, err)
	fmt.Println(table.String())
}
