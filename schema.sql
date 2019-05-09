-- ----------------------------
--  Table structure for `meta_connections`
-- ----------------------------
DROP TABLE IF EXISTS `meta_connections`;
CREATE TABLE `meta_connections` (
  `connection_id` char(2) NOT NULL DEFAULT '',
  `connection_name` varchar(255) DEFAULT NULL COMMENT '业务数据库中文名称',
  `db_type` varchar(255) NOT NULL COMMENT '数据库类型',
  `host` varchar(255) NOT NULL COMMENT '主机名',
  `host_map` varchar(255) DEFAULT NULL COMMENT '主机名map',
  `default_db` varchar(255) NOT NULL COMMENT '数据库名',
  `user` varchar(255) NOT NULL COMMENT '用户名',
  `password` varchar(255) NOT NULL COMMENT '密码',
  `port` int(11) NOT NULL COMMENT '端口',
  `jdbc_extend` varchar(255) DEFAULT NULL COMMENT 'jdbc扩展参数',
  `use_direct` varchar(255) DEFAULT NULL COMMENT '是否使用mysql的direct',
  `all_table_nums` int(11) DEFAULT NULL COMMENT '表总数',
  `init_time` datetime DEFAULT NULL COMMENT '统计表总数的执行时间',
  `status` char(1) DEFAULT '1' COMMENT '是否生效，默认1为有效，0为无效',
  `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `remark` varchar(2000) DEFAULT NULL COMMENT '备注',
  PRIMARY KEY (`connection_id`),
  UNIQUE KEY `unique_name` (`connection_name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='数据库配置信息';

-- ----------------------------
--  Table structure for `meta_export`
-- ----------------------------
DROP TABLE IF EXISTS `meta_export`;
CREATE TABLE `meta_export` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `hive_database` varchar(60) NOT NULL COMMENT 'hive数据库',
  `hive_table` varchar(100) NOT NULL COMMENT 'hive表名',
  `connection_id` char(2) NOT NULL,
  `db_name` varchar(60) NOT NULL,
  `table_name` varchar(100) NOT NULL COMMENT '表名称',
  `exec_engine` varchar(10) DEFAULT 'sqoop' COMMENT '执行引擎，sqoop或者datax，默认值sqoop',
  `is_overwrite` char(1) DEFAULT '1' COMMENT '是否覆盖，1为覆盖，0为不覆盖。（truncate）',
  `columns` varchar(4000) DEFAULT NULL COMMENT '导入的字段',
  `use_raw_null` char(1) DEFAULT '1' COMMENT '是否使用原始NULL值，1为使用，0为不使用',
  `m` tinyint(4) DEFAULT NULL COMMENT 'map数，默认为空即为1',
  `mode` varchar(20) DEFAULT 'rename' COMMENT '导入模式，[rename、overwrite、append] 默认rename',
  `last_exec_date` char(10) DEFAULT NULL COMMENT '任务最后执行日期',
  `retry_count` tinyint(4) DEFAULT '3' COMMENT '任务失败重试次数',
  `task_order` int(11) DEFAULT NULL COMMENT '任务执行顺序',
  `table_rows` int(11) DEFAULT NULL COMMENT '表行数',
  `init_time` datetime DEFAULT NULL COMMENT '统计表行数的执行时间',
  `is_drop` char(1) DEFAULT '0' COMMENT '是否删除hive中的表，默认0为不删除，1为删除。（目前没用）',
  `is_bak_task` char(1) DEFAULT '0' COMMENT '是否备份任务。默认为0，1为备份工作的任务',
  `is_dev_task` char(1) DEFAULT '0' COMMENT '是否开发任务。默认为0，1为开发工作的任务',
  `status` char(1) DEFAULT '1' COMMENT '是否生效，默认1为有效，0为无效',
  `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `remark` varchar(255) DEFAULT NULL COMMENT '备注',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_unique` (`connection_id`,`db_name`,`table_name`) USING BTREE
) ENGINE=InnoDB CHARSET=utf8 COMMENT='导出表配置信息';

-- ----------------------------
--  Table structure for `meta_import`
-- ----------------------------
DROP TABLE IF EXISTS `meta_import`;
CREATE TABLE `meta_import` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `connection_id` char(2) NOT NULL,
  `db_name` varchar(60) NOT NULL,
  `table_name` varchar(100) NOT NULL COMMENT '表名称',
  `hive_database` varchar(60) NOT NULL COMMENT 'hive数据库',
  `hive_table` varchar(100) NOT NULL COMMENT 'hive表名',
  `exec_engine` varchar(10) NOT NULL DEFAULT 'sqoop' COMMENT '执行引擎，sqoop或者datax，默认值sqoop',
  `is_overwrite` char(1) DEFAULT '0' COMMENT '是否覆盖，1为覆盖，0为不覆盖',
  `query_sql` varchar(4000) DEFAULT NULL COMMENT 'datax的querySql参数，目前不支持sqoop和filter。当用户配置querySql时，MysqlReader直接忽略table、column、where条件的配置，querySql优先级大于table、column、where选项。',
  `columns` varchar(4000) DEFAULT NULL COMMENT '导入的字段',
  `filter` varchar(255) DEFAULT NULL COMMENT 'where条件',
  `max_value` varchar(255) DEFAULT NULL COMMENT 'where部分的增量值，必须使用$max_time命名变量',
  `map_column_hive` varchar(255) DEFAULT NULL COMMENT '字段映射类型',
  `hive_partition_key` varchar(255) DEFAULT 'date' COMMENT '分区字段',
  `hive_partition_value` varchar(255) DEFAULT '$yesterday' COMMENT '分区值',
  `fields_terminated_by` varchar(255) DEFAULT NULL COMMENT '分隔符',
  `line_terminated_by` varchar(255) DEFAULT NULL COMMENT '换行符',
  `use_raw_null` char(1) DEFAULT '1' COMMENT '是否使用原始NULL值，1为使用，0为不使用',
  `use_local_mode` char(1) DEFAULT '0' COMMENT '是否使用本地模式-jt local，1为使用，0不适用，默认0',
  `warehouse_dir` varchar(255) DEFAULT NULL COMMENT '数据临时存储目录，不指定默认是当前用户目录下的表名。如果存在相同的表名不同的库名同时执行就会存在问题',
  `class_name` varchar(255) DEFAULT NULL COMMENT '指定生产jar文件的名称',
  `outdir` varchar(255) DEFAULT NULL COMMENT '指定生成jar文件的路径',
  `split_by` varchar(255) DEFAULT NULL,
  `m` tinyint(4) DEFAULT NULL COMMENT 'map数，默认为空即为1',
  `last_exec_date` char(10) CHARACTER SET latin1 DEFAULT NULL COMMENT '任务最后执行日期',
  `retry_count` tinyint(4) DEFAULT '3' COMMENT '任务失败重试次数',
  `task_order` int(11) DEFAULT NULL COMMENT '任务执行顺序',
  `table_rows` int(11) DEFAULT NULL COMMENT '表行数',
  `rows_updatetime` varchar(60) DEFAULT NULL COMMENT '更新字段的时间',
  `is_drop` char(1) DEFAULT '0' COMMENT '是否删除hive中的表，默认0为不删除，1为删除。（目前没用）',
  `is_bak_task` char(1) DEFAULT '0' COMMENT '是否备份任务。默认为0，1为备份工作的任务',
  `is_dev_task` char(1) DEFAULT '0' COMMENT '是否开发任务。默认为0，1为开发工作的任务',
  `status` char(1) DEFAULT '1' COMMENT '是否生效，默认1为有效，0为无效',
  `create_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '修改时间',
  `remark` varchar(2000) DEFAULT NULL COMMENT '备注',
  PRIMARY KEY (`id`),
  UNIQUE KEY `idx_unique` (`connection_id`,`db_name`,`table_name`) USING BTREE
) ENGINE=InnoDB CHARSET=utf8 COMMENT='导入表配置信息';

