package com.phantom.spark.sparkproject.meituan.dao;

import java.util.List;

import com.phantom.spark.sparkproject.meituan.domain.AdStat;

/**
 * 广告实时统计DAO接口
 * @author Administrator
 *
 */
public interface IAdStatDAO {

	void updateBatch(List<AdStat> adStats);
	
}
