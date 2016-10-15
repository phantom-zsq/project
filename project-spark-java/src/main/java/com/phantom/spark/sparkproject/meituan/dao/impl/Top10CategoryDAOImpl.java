package com.phantom.spark.sparkproject.meituan.dao.impl;

import com.phantom.spark.sparkproject.meituan.dao.ITop10CategoryDAO;
import com.phantom.spark.sparkproject.meituan.domain.Top10Category;
import com.phantom.spark.sparkproject.meituan.jdbc.JDBCHelper;

/**
 * top10品类DAO实现
 * @author Administrator
 *
 */
public class Top10CategoryDAOImpl implements ITop10CategoryDAO {

	public void insert(Top10Category category) {
		String sql = "insert into top10_category values(?,?,?,?,?)";  
		
		Object[] params = new Object[]{category.getTaskid(),
				category.getCategoryid(),
				category.getClickCount(),
				category.getOrderCount(),
				category.getPayCount()};  
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
