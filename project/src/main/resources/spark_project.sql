/*
Navicat MySQL Data Transfer

Source Server         : localhost_3306
Source Server Version : 50513
Source Host           : localhost:3306
Source Database       : spark_project

Target Server Type    : MYSQL
Target Server Version : 50513
File Encoding         : 65001

Date: 2016-10-14 20:31:08
*/

SET FOREIGN_KEY_CHECKS=0;

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
  `taskid` bigint(20) NOT NULL,
  `taskName` varchar(255) DEFAULT NULL,
  `createTime` varchar(255) DEFAULT NULL,
  `startTime` varchar(255) DEFAULT NULL,
  `finishTime` varchar(255) DEFAULT NULL,
  `taskType` varchar(255) DEFAULT NULL,
  `taskStatus` varchar(255) DEFAULT NULL,
  `taskParam` varchar(10000) DEFAULT NULL,
  PRIMARY KEY (`taskid`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of task
-- ----------------------------
INSERT INTO `task` VALUES ('1', '', null, null, null, null, null, '');
INSERT INTO `task` VALUES ('2', 'session', '2016-10-12', '2016-10-12', '2016-10-12', 'session', '0', '{\"startDate\":\"2016-10-01\",\"endDate\":\"2017-10-01\",\"startAge\":\"20\",\"endAge\":\"30\",\"professionals\":\"professional1\",\"cities\":\"city1,city2,city3,city4,city5,city6,city7,city8,city9,city10\",\"sex\":\"male\",\"keywords\":\"温泉\",\"categoryIds\":\"1\",\"targetPageFlow\":\"1\"}');
INSERT INTO `task` VALUES ('3', 'product', '2016-10-13', '2016-10-13', '2016-10-13', 'product', '0', '{\"startDate\":\"2016-10-01\",\"endDate\":\"2017-10-01\",\"startAge\":\"20\",\"endAge\":\"30\",\"professionals\":\"professional1\",\"cities\":\"city1,city2,city3,city4,city5,city6,city7,city8,city9,city10\",\"sex\":\"male\",\"keywords\":\"温泉\",\"categoryIds\":\"1\",\"targetPageFlow\":\"1\"}');
INSERT INTO `task` VALUES ('4', 'page', '2016-10-14', '2016-10-14', '2016-10-14', 'page', '0', '{\"startDate\":\"2016-10-01\",\"endDate\":\"2017-10-01\",\"startAge\":\"20\",\"endAge\":\"30\",\"professionals\":\"professional1\",\"cities\":\"city1,city2,city3,city4,city5,city6,city7,city8,city9,city10\",\"sex\":\"male\",\"keywords\":\"温泉\",\"categoryIds\":\"1\",\"targetPageFlow\":\"1\"}');
