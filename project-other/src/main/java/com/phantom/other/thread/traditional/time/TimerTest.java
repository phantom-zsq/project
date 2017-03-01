package com.phantom.other.thread.traditional.time;

import java.util.Calendar;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;

public class TimerTest {

	private static final long PERIOD_DAY = 5000;//24 * 60 * 60 * 1000;

	public static void main(String[] args) {

		Calendar calendar = Calendar.getInstance();
		calendar.set(Calendar.HOUR_OF_DAY, 10); // 16点
		calendar.set(Calendar.MINUTE, 56);
		calendar.set(Calendar.SECOND, 0);
		Date date = calendar.getTime(); // 第一次执行定时任务的时间
		// 如果第一次执行定时任务的时间小于当前的时间
		// 此时要在第一次执行定时任务的时间加一天，以便此任务在下个时间点执行。如果不加一天，任务会立即执行。
		if (date.before(new Date())) {
			date = addDay(date, 1);
		}
		System.out.println(date);
		// 安排指定的任务在指定的时间开始进行重复的固定延迟执行。
		new Timer().schedule(new MyTimerTask(), date, PERIOD_DAY);
		while (true) {
			System.out.println(new Date().getSeconds());
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	// 增加或减少天数
	public static Date addDay(Date date, int num) {

		Calendar startDT = Calendar.getInstance();
		startDT.setTime(date);
		startDT.add(Calendar.DAY_OF_MONTH, num);
		return startDT.getTime();
	}
}

class MyTimerTask extends TimerTask {

	@Override
	public void run() {

		System.out.println("bombing!");
	}
}
