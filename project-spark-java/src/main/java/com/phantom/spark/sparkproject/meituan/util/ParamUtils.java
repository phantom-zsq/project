package com.phantom.spark.sparkproject.meituan.util;

import com.alibaba.fastjson.JSONObject;
import com.phantom.spark.sparkproject.meituan.conf.ConfigurationManager;
import com.phantom.spark.sparkproject.meituan.constant.Constants;

/**
 * 参数工具类
 * @author Administrator
 *
 */
public class ParamUtils {

	/**
	 * 从命令行参数中提取任务id
	 * @param args 命令行参数
	 * @return 任务id
	 */
	public static Long getTaskIdFromArgs(String[] args, String taskType) {
		boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
		
		if(local) {
			return ConfigurationManager.getLong(taskType);  
		} else {
			try {
				if(args != null && args.length > 0) {
					return Long.valueOf(args[0]);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return null;
	}
	
	/**
	 * 从JSON对象中提取参数
	 * @param jsonObject JSON对象
	 * @return 参数
	 */
	public static String getParam(JSONObject jsonObject, String field) {
		
		return jsonObject.getString(field);
	}
}
