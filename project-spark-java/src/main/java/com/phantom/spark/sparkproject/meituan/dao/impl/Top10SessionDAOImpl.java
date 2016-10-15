package com.phantom.spark.sparkproject.meituan.dao.impl;

import com.phantom.spark.sparkproject.meituan.dao.ITop10SessionDAO;
import com.phantom.spark.sparkproject.meituan.domain.Top10Session;
import com.phantom.spark.sparkproject.meituan.jdbc.JDBCHelper;

/**
 * top10活跃session的DAO实现
 * @author Administrator
 *
 */
public class Top10SessionDAOImpl implements ITop10SessionDAO {

	public void insert(Top10Session top10Session) {
		String sql = "insert into top10_session values(?,?,?,?)"; 
		
		Object[] params = new Object[]{top10Session.getTaskid(),
				top10Session.getCategoryid(),
				top10Session.getSessionid(),
				top10Session.getClickCount()};
		
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		jdbcHelper.executeUpdate(sql, params);
	}

}
