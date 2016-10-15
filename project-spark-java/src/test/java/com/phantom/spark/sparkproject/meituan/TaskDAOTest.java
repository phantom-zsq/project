package com.phantom.spark.sparkproject.meituan;

import com.phantom.spark.sparkproject.meituan.dao.ITaskDAO;
import com.phantom.spark.sparkproject.meituan.dao.factory.DAOFactory;
import com.phantom.spark.sparkproject.meituan.domain.Task;

/**
 * 任务管理DAO测试类
 * @author Administrator
 *
 */
public class TaskDAOTest {
	
	public static void main(String[] args) {
		ITaskDAO taskDAO = DAOFactory.getTaskDAO();
		Task task = taskDAO.findById(2);
		System.out.println(task.getTaskName());  
	}
	
}
