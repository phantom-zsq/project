package com.phantom.spark.sparkproject.meituan.dao;

import java.util.List;

import com.phantom.spark.sparkproject.meituan.domain.AdProvinceTop3;

/**
 * 各省份top3热门广告DAO接口
 * @author Administrator
 *
 */
public interface IAdProvinceTop3DAO {

	void updateBatch(List<AdProvinceTop3> adProvinceTop3s);
	
}
