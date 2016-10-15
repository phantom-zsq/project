package com.phantom.hadoop.hadoopproject.lvmama.mr.etl;

import java.io.IOException;
import java.util.Calendar;
import java.util.Map;
import java.util.zip.CRC32;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.phantom.hadoop.hadoopproject.lvmama.common.EventLogConstants;
import com.phantom.hadoop.hadoopproject.lvmama.common.EventLogConstants.EventEnum;
import com.phantom.hadoop.hadoopproject.lvmama.common.EventLogConstants.PlatformNameConstants;
import com.phantom.hadoop.hadoopproject.lvmama.common.GlobalConstants;
import com.phantom.hadoop.hadoopproject.lvmama.util.LoggerUtil;
import com.phantom.hadoop.hadoopproject.lvmama.util.TimeUtil;

/**
 * etl操作的mapper类
 * 
 * @author 张少奇
 * @time 2016年10月15日 下午5:19:46
 */
public class AnalyserLogDataMapper extends Mapper<Object, Text, NullWritable, Put> {

	private static final Logger logger = Logger.getLogger(AnalyserLogDataMapper.class);
	private CRC32 crc32 = new CRC32();
	private long currentDayMillis = 0;

	/**
	 * Called once at the beginning of the task.
	 */
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		Configuration conf = context.getConfiguration();
		String date = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);

		// 创建一个日历对象
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(TimeUtil.parseString2Long(date));
		currentDayMillis = cal.getTimeInMillis();
	}

	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		logger.debug("analyse log of data:" + value);

		// 日志解析
		Map<String, String> clientInfo = LoggerUtil.handleLog(value.toString());

		// 过滤无效数据
		if (clientInfo.isEmpty()) {
			logger.debug("过滤无效数据" + value);
			return;
		}

		// 获取事件名称，按照不同事件进行处理
		String eventAliasName = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME);
		EventEnum event = EventEnum.valueOfAlias(eventAliasName);
		switch (event) {
		case CHARGEREFUND:
		case CHARGEREQUEST:
		case CHARGESUCCESS:
		case EVENT:
		case LAUNCH:
		case PAGEVIEW:
			// 只处理这六种event数据
			this.handleData(clientInfo, event, context, value.toString());
			break;
		default:
			logger.debug("不能处理该event数据" + eventAliasName);
			break;
		}
	}

	/**
	 * 处理数据
	 * 
	 * @param clientInfo
	 * @param event
	 * @param context
	 * @param logStr
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void handleData(Map<String, String> clientInfo, EventEnum event, Context context, String logStr)
			throws IOException, InterruptedException {

		String platform = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_PLATFORM);
		String uuid = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_UUID);
		String serverTime = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME);

		// 检测无效数据
		if (!this.checkFieldIntegrity(event, platform, clientInfo)) {
			logger.debug("数据不完整" + logStr);
			return;
		}

		// 去除无用数据
		clientInfo.remove(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT);

		// rowkey的生产
		byte[] rowkey = this.generateRowKey(uuid, Long.valueOf(serverTime), clientInfo);
		// put对象创建
		Put put = new Put(rowkey);
		for (Map.Entry<String, String> entry : clientInfo.entrySet()) {
			if (StringUtils.isNotBlank(entry.getKey()) && StringUtils.isNotBlank(entry.getValue())) {
				put.add(EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME, Bytes.toBytes(entry.getKey()),
						Bytes.toBytes(entry.getValue()));
			}
		}
		// 输出
		context.write(NullWritable.get(), put);
	}

	/**
	 * 产生rowkey
	 * 
	 * @author 张少奇
	 * @time 2016年10月15日 下午5:42:43
	 * @param uuid
	 * @param serverTime
	 * @param clientInfo
	 * @return
	 */
	private byte[] generateRowKey(String uuid, long serverTime, Map<String, String> clientInfo) {

		byte[] bf1 = Bytes.toBytes((int) (serverTime - this.currentDayMillis)); // 当天的过去的毫秒数

		this.crc32.reset(); // 重置
		if (StringUtils.isNotBlank(uuid)) {
			this.crc32.update(uuid.getBytes());
		}
		this.crc32.update(Bytes.toBytes(clientInfo.hashCode()));
		byte[] bf2 = Bytes.toBytes(this.crc32.getValue());

		byte[] buffer = new byte[bf1.length + bf2.length];
		System.arraycopy(bf1, 0, buffer, 0, bf1.length);
		System.arraycopy(bf2, 0, buffer, bf1.length, bf2.length);
		return buffer;
	}

	/**
	 * 检查数据的完整性，如果数据完整返回true，否则返回false
	 * 
	 * @author 张少奇
	 * @time 2016年10月15日 下午5:42:50
	 * @param event
	 * @param platform
	 * @param clientInfo
	 * @return
	 */
	private boolean checkFieldIntegrity(EventEnum event, String platform, Map<String, String> clientInfo) {

		boolean result = StringUtils.isNotBlank(platform) && event != null
				&& StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME));

		if (result) {
			// 针对具体的event进行字段信息的判断
			String uuid = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_UUID);
			String sessionId = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_SESSION_ID);
			String memberId = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_MEMBER_ID);
			String orderId = clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_ORDER_ID);

			switch (platform) {
			case PlatformNameConstants.PC_WEBSITE_SDK:
				result = result && StringUtils.isNotBlank(uuid) && StringUtils.isNotBlank(sessionId);
				// 处理不同event的
				switch (event) {
				case CHARGEREQUEST: // 订单产生事件
					result = result && StringUtils.isNotBlank(orderId)
							&& StringUtils
									.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_ORDER_CURRENCY_TYPE))
							&& StringUtils
									.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_ORDER_PAYMENT_TYPE))
							&& StringUtils.isNotBlank(EventLogConstants.LOG_COLUMN_NAME_ORDER_CURRENCY_AMOUNT);
					break;
				case PAGEVIEW:
					result = result
							&& StringUtils.isNotBlank(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_CURRENT_URL));
					break;
				case LAUNCH:
					// 没有特殊要求
					break;
				case EVENT:
					result = result && StringUtils.isNotBlank(EventLogConstants.LOG_COLUMN_NAME_EVENT_CATEGORY)
							&& StringUtils.isNotBlank(EventLogConstants.LOG_COLUMN_NAME_EVENT_ACTION);
					break;
				default:
					// 不应该出现其他事件类型
					result = false;
					break;
				}
				break;
			case PlatformNameConstants.JAVA_SERVER_SDK:
				result = result && StringUtils.isNotBlank(memberId);
				switch (event) {
				case CHARGESUCCESS:
				case CHARGEREFUND:
					result = result && StringUtils.isNotBlank(orderId);
					break;
				default:
					// 不应该出现此event
					result = false;
					break;
				}
				break;
			default:
				// 不应该出现的平台
				result = false;
				break;
			}
		}
		return result;
	}
}