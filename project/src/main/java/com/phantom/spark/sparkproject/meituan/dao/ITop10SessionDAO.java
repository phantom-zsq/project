package com.phantom.spark.sparkproject.meituan.dao;

import com.phantom.spark.sparkproject.meituan.domain.Top10Session;

/**
 * top10活跃session的DAO接口
 * @author Administrator
 *
 */
public interface ITop10SessionDAO {

	void insert(Top10Session top10Session);
	
}
