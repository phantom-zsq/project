package com.phantom.hadoop.hadoopproject.lvmama.mr.stats.nu;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.phantom.hadoop.hadoopproject.lvmama.common.DateEnum;
import com.phantom.hadoop.hadoopproject.lvmama.common.EventLogConstants;
import com.phantom.hadoop.hadoopproject.lvmama.common.EventLogConstants.EventEnum;
import com.phantom.hadoop.hadoopproject.lvmama.common.GlobalConstants;
import com.phantom.hadoop.hadoopproject.lvmama.common.KpiType;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.basic.BrowserDimension;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.basic.DateDimension;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.basic.KpiDimension;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.basic.PlatformDimension;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.stats.StatsCommonDimension;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.stats.StatsUserDimension;
import com.phantom.hadoop.hadoopproject.lvmama.util.TimeUtil;

/**
 * 自定义的计算新增访客的mapper类
 * 
 * @author ibf
 *
 */
public class NewInstallUserMapper extends TableMapper<StatsUserDimension, Text> {
    private static final Logger logger = Logger.getLogger(NewInstallUserMapper.class);
    private byte[] family = EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME;
    private Text userId = new Text();
    private StatsUserDimension statsUserDimension = new StatsUserDimension();
    private BrowserDimension defaultBrowserDimension = new BrowserDimension("", "");
    private KpiDimension newInstallUserKpiDimension = new KpiDimension(
            KpiType.NEW_INSTALL_USER.name);
    private KpiDimension browserNewInstallUserKpiDimension = new KpiDimension(
            KpiType.BROWSER_NEW_INSTALL_USER.name);

    long date, endOfDate;
    long firstDayOfThisWeek, firstDayOfNextWeek;
    long firstDayOfThisMonth, firstDayOfNextMonth;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        date = TimeUtil.parseString2Long(conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        endOfDate = TimeUtil.getSpecifiedDate(date, 1);
        firstDayOfThisWeek = TimeUtil.getFirstDayOfThisWeek(date);
        firstDayOfThisMonth = TimeUtil.getFirstDayOfThisMonth(date);
        firstDayOfNextWeek = TimeUtil.getFirstDayOfNextWeek(date);
        firstDayOfNextMonth = TimeUtil.getFirstDayOfNextMonth(date);
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
            throws IOException, InterruptedException {
        String uuid = Bytes.toString(
                value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_UUID)));
        String serverTime = Bytes.toString(value.getValue(family,
                Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));
        String paltform = Bytes.toString(
                value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)));
        String version = Bytes.toString(
                value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_VERSION)));
        String browser = Bytes.toString(value.getValue(family,
                Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME)));
        String browserVersion = Bytes.toString(value.getValue(family,
                Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION)));
        String eventName = Bytes.toString(value.getValue(family,
                Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME)));

        // 过滤无效数据
        if (StringUtils.isBlank(uuid) || StringUtils.isBlank(serverTime)
                || !StringUtils.isNumeric(serverTime) || StringUtils.isBlank(paltform)
                || !EventEnum.LAUNCH.alias.equals(eventName)) {
            logger.debug("数据补全，直接过滤");
            return;
        }

        long longOfServerTime = Long.valueOf(serverTime);
        this.userId.set(uuid); // 设置uuid

        // 开始创建时间维度
        DateDimension dayDimension = DateDimension.buildDate(longOfServerTime, DateEnum.DAY);
        DateDimension weekDimension = DateDimension.buildDate(longOfServerTime, DateEnum.WEEK);
        DateDimension monthDimension = DateDimension.buildDate(longOfServerTime, DateEnum.MONTH);

        // 创建平台维度
        List<PlatformDimension> platforms = PlatformDimension.buildList(paltform, version);

        // 创建浏览器维度
        List<BrowserDimension> browsers = BrowserDimension.buildList(browser, browserVersion);

        // 数据进行输出
        StatsCommonDimension statsCommonDimension = this.statsUserDimension.getStatsCommon();
        for (PlatformDimension pf : platforms) {
            // 1、处理stats_user表对应的数据，也就是说不包含browser维度的数据
            this.statsUserDimension.setBrowser(this.defaultBrowserDimension); // 占位使用
            statsCommonDimension.setPlatform(pf);
            statsCommonDimension.setKpi(this.newInstallUserKpiDimension);

            // 天维度
            if (longOfServerTime >= date && longOfServerTime < endOfDate) {
                statsCommonDimension.setDate(dayDimension);
                context.write(statsUserDimension, userId);
            }

            // 周维度
            if (longOfServerTime >= firstDayOfThisWeek && longOfServerTime < firstDayOfNextWeek) {
                statsCommonDimension.setDate(weekDimension);
                context.write(statsUserDimension, userId);
            }

            // 月维度
            if (longOfServerTime >= firstDayOfThisMonth && longOfServerTime < firstDayOfNextMonth) {
                statsCommonDimension.setDate(monthDimension);
                context.write(statsUserDimension, userId);
            }

            // 2、处理stats_device_browser表对应的数据，也就是说包含了browser维度的数据
            for (BrowserDimension br : browsers) {
                this.statsUserDimension.setBrowser(br); // 具体的维度
                statsCommonDimension.setKpi(this.browserNewInstallUserKpiDimension);

                // 天维度
                if (longOfServerTime >= date && longOfServerTime < endOfDate) {
                    statsCommonDimension.setDate(dayDimension);
                    context.write(statsUserDimension, userId);
                }

                // 周维度
                if (longOfServerTime >= firstDayOfThisWeek
                        && longOfServerTime < firstDayOfNextWeek) {
                    statsCommonDimension.setDate(weekDimension);
                    context.write(statsUserDimension, userId);
                }

                // 月维度
                if (longOfServerTime >= firstDayOfThisMonth
                        && longOfServerTime < firstDayOfNextMonth) {
                    statsCommonDimension.setDate(monthDimension);
                    context.write(statsUserDimension, userId);
                }
            }
        }
    }
}
