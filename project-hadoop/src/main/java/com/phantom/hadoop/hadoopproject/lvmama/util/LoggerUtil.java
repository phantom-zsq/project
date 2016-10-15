package com.phantom.hadoop.hadoopproject.lvmama.util;

import java.net.URLDecoder;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.phantom.hadoop.hadoopproject.lvmama.common.EventLogConstants;
import com.phantom.hadoop.hadoopproject.lvmama.util.IPSeekerExt.RegionInfo;

import cz.mallat.uasparser.UserAgentInfo;

/**
 * 日志抽取工具类
 * @author 张少奇
 * @time 2016年10月15日 下午5:30:57
 */
public class LoggerUtil {
	
    private static final Logger logger = Logger.getLogger(LoggerUtil.class);
    private static final IPSeekerExt ipSeeker = IPSeekerExt.getInstance();

    /**
     * 处理日志，返回一个map集合，如果处理失败，那么empty的map集合<br/>
     * 失败：分割失败，url解析出现问题等等
     * @author 张少奇
     * @time 2016年10月15日 下午5:31:47 
     * @param logText
     * @return
     */
    public static Map<String, String> handleLog(String logText) {
    	
        Map<String, String> clientInfo = new HashMap<String, String>();
        if (StringUtils.isNotBlank(logText)) {
            String[] splits = logText.trim().split(EventLogConstants.LOG_SEPARTIOR);
            if (splits.length == 3) {
                // 数据格式正确，可以分割
                // IP地址
                clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_IP, splits[0].trim());
                // 服务器时间
                long nginxTime = TimeUtil.parseNginxServerTime2Long(splits[1].trim());
                if (nginxTime != -1) {
                    clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME,
                            String.valueOf(nginxTime));
                }
                // 处理请求参数
                String requestStr = splits[2];
                int index = requestStr.indexOf("?");
                if (index > -1) {
                    // 有请求参数的情况下
                    String requestBody = requestStr.substring(index + 1);
                    // 处理请求参数
                    handleRequestBody(clientInfo, requestBody);
                    // 添加IP解析功能
                    RegionInfo info = ipSeeker.analysisIp(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_IP));
                    clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_CITY, info.getCity());
                    clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_PROVINCE, info.getProvince());
                    clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_COUNTRY, info.getCountry());
                    //:TODO 后面ip解析之类的东西
                    UserAgentInfo userAgentInfo = UserAgentUtil.analyticUserAgent(clientInfo.get(EventLogConstants.LOG_COLUMN_NAME_USER_AGENT));
                    if (userAgentInfo != null) {
                        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, userAgentInfo.getUaFamily());
                        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION, userAgentInfo.getBrowserVersionInfo());
                        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_OS_NAME, userAgentInfo.getOsFamily());
                        clientInfo.put(EventLogConstants.LOG_COLUMN_NAME_OS_VERSION, userAgentInfo.getOsName());
                    }
                } else {
                    // 没有请求参数
                    clientInfo.clear();
                }
            }
        }
        return clientInfo;
    }

    /**
     * 处理我们的请求主体部分代码
     * @author 张少奇
     * @time 2016年10月15日 下午5:32:06 
     * @param clientInfo
     * @param requestBody
     */
    private static void handleRequestBody(Map<String, String> clientInfo, String requestBody) {
    	
        String[] requestParames = requestBody.split("&");
        for (String parame : requestParames) {
            if (StringUtils.isNotBlank(parame)) {
                int index = parame.indexOf("=");
                if (index < 0) {
                    logger.debug("没法进行解析:" + parame);
                    continue;
                }

                String key, value = null;
                try {
                    key = parame.substring(0, index);
                    value = URLDecoder.decode(parame.substring(index + 1), "utf-8");
                } catch (Exception e) {
                    logger.debug("value值decode时候出现异常", e);
                    continue;
                }

                if (StringUtils.isNotBlank(key) && StringUtils.isNotBlank(value)) {
                    clientInfo.put(key, value);
                }
            }
        }
    }
}