package com.phantom.spark.sparkproject.meituan.dao.impl;

import com.phantom.spark.sparkproject.meituan.dao.IPageSplitConvertRateDAO;
import com.phantom.spark.sparkproject.meituan.domain.PageSplitConvertRate;
import com.phantom.spark.sparkproject.meituan.jdbc.JDBCHelper;

/**
 * 页面切片转化率DAO实现类
 * @author Administrator
 *
 */
public class PageSplitConvertRateDAOImpl implements IPageSplitConvertRateDAO {

	public void insert(PageSplitConvertRate pageSplitConvertRate) {
		String sql = "insert into page_split_convert_rate values(?,?)";  
		Object[] params = new Object[]{pageSplitConvertRate.getTaskid(), 
				pageSplitConvertRate.getConvertRate()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
