package com.phantom.spark.sparkproject.meituan.dao.factory;

import com.phantom.spark.sparkproject.meituan.dao.IAdBlacklistDAO;
import com.phantom.spark.sparkproject.meituan.dao.IAdClickTrendDAO;
import com.phantom.spark.sparkproject.meituan.dao.IAdProvinceTop3DAO;
import com.phantom.spark.sparkproject.meituan.dao.IAdStatDAO;
import com.phantom.spark.sparkproject.meituan.dao.IAdUserClickCountDAO;
import com.phantom.spark.sparkproject.meituan.dao.IAreaTop3ProductDAO;
import com.phantom.spark.sparkproject.meituan.dao.IPageSplitConvertRateDAO;
import com.phantom.spark.sparkproject.meituan.dao.ISessionAggrStatDAO;
import com.phantom.spark.sparkproject.meituan.dao.ISessionDetailDAO;
import com.phantom.spark.sparkproject.meituan.dao.ISessionRandomExtractDAO;
import com.phantom.spark.sparkproject.meituan.dao.ITaskDAO;
import com.phantom.spark.sparkproject.meituan.dao.ITop10CategoryDAO;
import com.phantom.spark.sparkproject.meituan.dao.ITop10SessionDAO;
import com.phantom.spark.sparkproject.meituan.dao.impl.AdBlacklistDAOImpl;
import com.phantom.spark.sparkproject.meituan.dao.impl.AdClickTrendDAOImpl;
import com.phantom.spark.sparkproject.meituan.dao.impl.AdProvinceTop3DAOImpl;
import com.phantom.spark.sparkproject.meituan.dao.impl.AdStatDAOImpl;
import com.phantom.spark.sparkproject.meituan.dao.impl.AdUserClickCountDAOImpl;
import com.phantom.spark.sparkproject.meituan.dao.impl.AreaTop3ProductDAOImpl;
import com.phantom.spark.sparkproject.meituan.dao.impl.PageSplitConvertRateDAOImpl;
import com.phantom.spark.sparkproject.meituan.dao.impl.SessionAggrStatDAOImpl;
import com.phantom.spark.sparkproject.meituan.dao.impl.SessionDetailDAOImpl;
import com.phantom.spark.sparkproject.meituan.dao.impl.SessionRandomExtractDAOImpl;
import com.phantom.spark.sparkproject.meituan.dao.impl.TaskDAOImpl;
import com.phantom.spark.sparkproject.meituan.dao.impl.Top10CategoryDAOImpl;
import com.phantom.spark.sparkproject.meituan.dao.impl.Top10SessionDAOImpl;

/**
 * DAO工厂类
 * @author Administrator
 *
 */
public class DAOFactory {


	public static ITaskDAO getTaskDAO() {
		return new TaskDAOImpl();
	}

	public static ISessionAggrStatDAO getSessionAggrStatDAO() {
		return new SessionAggrStatDAOImpl();
	}
	
	public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
		return new SessionRandomExtractDAOImpl();
	}
	
	public static ISessionDetailDAO getSessionDetailDAO() {
		return new SessionDetailDAOImpl();
	}
	
	public static ITop10CategoryDAO getTop10CategoryDAO() {
		return new Top10CategoryDAOImpl();
	}
	
	public static ITop10SessionDAO getTop10SessionDAO() {
		return new Top10SessionDAOImpl();
	}
	
	public static IPageSplitConvertRateDAO getPageSplitConvertRateDAO() {
		return new PageSplitConvertRateDAOImpl();
	}
	
	public static IAreaTop3ProductDAO getAreaTop3ProductDAO() {
		return new AreaTop3ProductDAOImpl();
	}
	
	public static IAdUserClickCountDAO getAdUserClickCountDAO() {
		return new AdUserClickCountDAOImpl();
	}
	
	public static IAdBlacklistDAO getAdBlacklistDAO() {
		return new AdBlacklistDAOImpl();
	}
	
	public static IAdStatDAO getAdStatDAO() {
		return new AdStatDAOImpl();
	}
	
	public static IAdProvinceTop3DAO getAdProvinceTop3DAO() {
		return new AdProvinceTop3DAOImpl();
	}
	
	public static IAdClickTrendDAO getAdClickTrendDAO() {
		return new AdClickTrendDAOImpl();
	}
	
}
