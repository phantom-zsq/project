package com.phantom.spark.sparkproject.meituan.dao;

import java.util.List;

import com.phantom.spark.sparkproject.meituan.domain.AdClickTrend;

/**
 * 广告点击趋势DAO接口
 * @author Administrator
 *
 */
public interface IAdClickTrendDAO {

	void updateBatch(List<AdClickTrend> adClickTrends);
	
}
