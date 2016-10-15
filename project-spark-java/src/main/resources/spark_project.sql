/*
Navicat MySQL Data Transfer

Source Server         : localhost_3306
Source Server Version : 50513
Source Host           : localhost:3306
Source Database       : spark_project

Target Server Type    : MYSQL
Target Server Version : 50513
File Encoding         : 65001

Date: 2016-10-14 21:13:54
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for area_top3_product
-- ----------------------------
DROP TABLE IF EXISTS `area_top3_product`;
CREATE TABLE `area_top3_product` (
  `task_id` bigint(20) NOT NULL,
  `area` varchar(255) DEFAULT NULL,
  `area_level` varchar(255) DEFAULT NULL,
  `product_id` bigint(20) DEFAULT NULL,
  `city_infos` varchar(255) DEFAULT NULL,
  `click_count` bigint(20) DEFAULT NULL,
  `product_name` varchar(255) DEFAULT NULL,
  `product_status` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of area_top3_product
-- ----------------------------

-- ----------------------------
-- Table structure for city_info
-- ----------------------------
DROP TABLE IF EXISTS `city_info`;
CREATE TABLE `city_info` (
  `city_id` bigint(20) NOT NULL,
  `city_name` varchar(255) DEFAULT NULL,
  `area` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`city_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of city_info
-- ----------------------------
INSERT INTO `city_info` VALUES ('0', '上海', '华东');
INSERT INTO `city_info` VALUES ('1', '北京', '华北');
INSERT INTO `city_info` VALUES ('2', '深圳', '华南');
INSERT INTO `city_info` VALUES ('3', '郑州', '华中');
INSERT INTO `city_info` VALUES ('4', '长沙', '华中');
INSERT INTO `city_info` VALUES ('5', '济南', '华东');
INSERT INTO `city_info` VALUES ('6', '武汉', '华中');
INSERT INTO `city_info` VALUES ('7', '成都', '西南');
INSERT INTO `city_info` VALUES ('8', '合肥', '华东');
INSERT INTO `city_info` VALUES ('9', '杭州', '华东');

-- ----------------------------
-- Table structure for task
-- ----------------------------
DROP TABLE IF EXISTS `task`;
CREATE TABLE `task` (
  `task_id` bigint(20) NOT NULL,
  `task_name` varchar(255) DEFAULT NULL,
  `create_time` varchar(255) DEFAULT NULL,
  `start_time` varchar(255) DEFAULT NULL,
  `finish_time` varchar(255) DEFAULT NULL,
  `task_type` varchar(255) DEFAULT NULL,
  `task_status` varchar(255) DEFAULT NULL,
  `task_param` varchar(10000) DEFAULT NULL,
  PRIMARY KEY (`task_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of task
-- ----------------------------
INSERT INTO `task` VALUES ('1', '', '', '', '', '', '', '');
INSERT INTO `task` VALUES ('2', 'session', '2016-10-12', '2016-10-12', '2016-10-12', 'session', '0', '{\"startDate\":\"2016-10-01\",\"endDate\":\"2017-10-01\",\"startAge\":\"20\",\"endAge\":\"30\",\"professionals\":\"professional1\",\"cities\":\"city1,city2,city3,city4,city5,city6,city7,city8,city9,city10\",\"sex\":\"male\",\"keywords\":\"温泉\",\"categoryIds\":\"1\",\"targetPageFlow\":\"1\"}');
INSERT INTO `task` VALUES ('3', 'product', '2016-10-13', '2016-10-13', '2016-10-13', 'product', '0', '{\"startDate\":\"2016-10-01\",\"endDate\":\"2017-10-01\",\"startAge\":\"20\",\"endAge\":\"30\",\"professionals\":\"professional1\",\"cities\":\"city1,city2,city3,city4,city5,city6,city7,city8,city9,city10\",\"sex\":\"male\",\"keywords\":\"温泉\",\"categoryIds\":\"1\",\"targetPageFlow\":\"1\"}');
INSERT INTO `task` VALUES ('4', 'page', '2016-10-14', '2016-10-14', '2016-10-14', 'page', '0', '{\"startDate\":\"2016-10-01\",\"endDate\":\"2017-10-01\",\"startAge\":\"20\",\"endAge\":\"30\",\"professionals\":\"professional1\",\"cities\":\"city1,city2,city3,city4,city5,city6,city7,city8,city9,city10\",\"sex\":\"male\",\"keywords\":\"温泉\",\"categoryIds\":\"1\",\"targetPageFlow\":\"1\"}');
