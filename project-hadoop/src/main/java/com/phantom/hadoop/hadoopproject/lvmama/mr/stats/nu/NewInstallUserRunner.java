package com.phantom.hadoop.hadoopproject.lvmama.mr.stats.nu;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.phantom.hadoop.hadoopproject.lvmama.common.DateEnum;
import com.phantom.hadoop.hadoopproject.lvmama.common.EventLogConstants;
import com.phantom.hadoop.hadoopproject.lvmama.common.EventLogConstants.EventEnum;
import com.phantom.hadoop.hadoopproject.lvmama.common.GlobalConstants;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.basic.DateDimension;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.stats.StatsUserDimension;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.value.MapWritableValue;
import com.phantom.hadoop.hadoopproject.lvmama.mr.TransformerOutputFormat;
import com.phantom.hadoop.hadoopproject.lvmama.util.JdbcManager;
import com.phantom.hadoop.hadoopproject.lvmama.util.TimeUtil;

public class NewInstallUserRunner implements Tool {
    private Configuration conf = null;

    public static void main(String[] args) {
        try {
            int exitCode = ToolRunner.run(new NewInstallUserRunner(), args);
            if (exitCode == 0) {
                System.out.println("执行成功");
            } else {
                System.out.println("执行失败");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setConf(Configuration conf) {
        // 为什么写在前面的主要原因是由于hbase创建config的时候，会进行加载
        conf.addResource("output-collector.xml");
        conf.addResource("query-mapping.xml");
        conf.addResource("transformer-env.xml");

        this.conf = HBaseConfiguration.create(conf);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        // 处理参数
        this.processArgs(conf, args);

        // 开始创建job
        Job job = Job.getInstance(conf, "new_install_user");
        // 设置job的相关信息, 一定不能少
        job.setJarByClass(NewInstallUserRunner.class);
        // 设置reduce
        job.setReducerClass(NewInstallUserReducer.class);
        // 输出reduce的输出key/value
        job.setOutputKeyClass(StatsUserDimension.class);
        job.setOutputValueClass(MapWritableValue.class);

        // 设置输出到hbase的设置
        this.setHBaseInputConfig(job);

        // 指定数据输出器
        job.setOutputFormatClass(TransformerOutputFormat.class);

        // 进行运行了
        if (job.waitForCompletion(true)) {
            // 执行成功，计算出当前维度的新增访客数量，开始计算总访客数量
            this.calculateTotalUsers(conf);
            return 0;
        } else {
            return -1;
        }
    }

    /**
     * 修改stats_user表中的这个总访客数量
     * 
     * @param conn
     * @param currentDateDimensionId
     * @param preDateDimensionId
     * @throws SQLException
     */
    private void updateStatsUserTotalUsers(Connection conn, int currentDateDimensionId,
            int preDateDimensionId) throws SQLException {
        if (currentDateDimensionId == -1) {
            return;
        }

        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Map<String, Integer> valueMap = new HashMap<>();
            // 1、获取上一个时间维度的值
            if (preDateDimensionId != -1) {
                // 上一个时间维度存在
                pstmt = conn.prepareStatement(
                        "select platform_dimension_id,date_dimension_id,total_install_users from stats_user where date_dimension_id = ?");
                pstmt.setInt(1, preDateDimensionId);
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    int platformDimensionId = rs.getInt("platform_dimension_id");
                    int totalUsers = rs.getInt("total_install_users");
                    valueMap.put(String.valueOf(platformDimensionId), totalUsers);
                }
            }

            // 关闭连接
            JdbcManager.closeConnection(null, pstmt, rs);

            // 2、获取当前时间维度的新增访客
            pstmt = conn.prepareStatement(
                    "select platform_dimension_id,date_dimension_id,new_install_users from stats_user where date_dimension_id = ?");
            pstmt.setInt(1, currentDateDimensionId);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                int platformDimensionId = rs.getInt("platform_dimension_id");
                int newUsers = rs.getInt("new_install_users");
                String key = String.valueOf(platformDimensionId);
                if (valueMap.containsKey(key)) {
                    newUsers += valueMap.get(key);
                }
                valueMap.put(key, newUsers);
            }

            // 关闭连接
            JdbcManager.closeConnection(null, pstmt, rs);

            // 3、插入数据库
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement("INSERT INTO `stats_user`(`platform_dimension_id`,"
                    + " `date_dimension_id`, `total_install_users`, `created`) "
                    + " VALUES(?, ?, ?, ?) ON DUPLICATE KEY UPDATE `total_install_users` = ?");
            int count = 0;
            for (Map.Entry<String, Integer> entry : valueMap.entrySet()) {
                pstmt.setInt(1, Integer.valueOf(entry.getKey()));
                pstmt.setInt(2, currentDateDimensionId);
                pstmt.setInt(3, entry.getValue());
                pstmt.setString(4, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
                pstmt.setInt(5, entry.getValue());
                pstmt.addBatch();
                if (++count > 500) {
                    pstmt.executeBatch();
                    conn.commit();
                }
            }
        } finally {
            try {
                pstmt.executeBatch();
                conn.commit();
                conn.setAutoCommit(true); // 还原
            } catch (Exception e) {
                // nothings
            } finally {
                JdbcManager.closeConnection(null, pstmt, rs);
            }
        }
    }

    /**
     * 更新stats_device_browser表的数据
     * 
     * @param conn
     * @param currentDateDimensionId
     * @param preDateDimensionId
     * @throws SQLException
     */
    private void updateStatsDeviceBrowserTotalUsers(Connection conn, int currentDateDimensionId,
            int preDateDimensionId) throws SQLException {
        if (currentDateDimensionId == -1) {
            return;
        }

        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Map<String, Integer> valueMap = new HashMap<>();
            // 1、获取上一个时间维度的值
            if (preDateDimensionId != -1) {
                // 上一个时间维度存在
                pstmt = conn.prepareStatement(
                        "select platform_dimension_id,browser_dimension_id,total_install_users from stats_device_browser where date_dimension_id = ?");
                pstmt.setInt(1, preDateDimensionId);
                rs = pstmt.executeQuery();
                while (rs.next()) {
                    int platformDimensionId = rs.getInt("platform_dimension_id");
                    int browserDimensionId = rs.getInt("browser_dimension_id");
                    int totalUsers = rs.getInt("total_install_users");
                    valueMap.put(platformDimensionId + "_" + browserDimensionId, totalUsers);
                }
            }

            // 关闭连接
            JdbcManager.closeConnection(null, pstmt, rs);

            // 2、获取当前时间维度的新增访客
            pstmt = conn.prepareStatement(
                    "select platform_dimension_id,browser_dimension_id,new_install_users from stats_device_browser where date_dimension_id = ?");
            pstmt.setInt(1, currentDateDimensionId);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                int platformDimensionId = rs.getInt("platform_dimension_id");
                int browserDimensionId = rs.getInt("browser_dimension_id");
                int newUsers = rs.getInt("new_install_users");
                String key = platformDimensionId + "_" + browserDimensionId;
                if (valueMap.containsKey(key)) {
                    newUsers += valueMap.get(key);
                }
                valueMap.put(key, newUsers);
            }

            // 关闭连接
            JdbcManager.closeConnection(null, pstmt, rs);

            // 3、插入数据库
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement("INSERT INTO `stats_device_browser`"
                    + "(`platform_dimension_id`,`browser_dimension_id`, "
                    + " `date_dimension_id`, `total_install_users`, `created`) "
                    + " VALUES(?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE `total_install_users` = ?");
            int count = 0;
            for (Map.Entry<String, Integer> entry : valueMap.entrySet()) {
                String[] splits = entry.getKey().split("_");
                pstmt.setInt(1, Integer.valueOf(splits[0]));
                pstmt.setInt(2, Integer.valueOf(splits[1]));
                pstmt.setInt(3, currentDateDimensionId);
                pstmt.setInt(4, entry.getValue());
                pstmt.setString(5, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
                pstmt.setInt(6, entry.getValue());
                pstmt.addBatch();
                if (++count > 500) {
                    pstmt.executeBatch();
                    conn.commit();
                }
            }
        } finally {
            try {
                pstmt.executeBatch();
                conn.commit();
                conn.setAutoCommit(true); // 还原
            } catch (Exception e) {
                // nothings
            } finally {
                JdbcManager.closeConnection(null, pstmt, rs);
            }
        }
    }

    /**
     * 根据指定的参数获取数据库中对于的维度id，如果数据库中不存在，那么返回-1
     * 
     * @param conn
     * @param dateDimension
     * @return
     * @throws SQLException
     */
    private int getDateDimensionId(Connection conn, DateDimension dateDimension)
            throws SQLException {
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            pstmt = conn.prepareStatement(
                    "SELECT `id` FROM `dimension_date` WHERE `year` = ? AND `season` = ? AND `month` = ? AND `week` = ? AND `day` = ? AND `type` = ? AND `calendar` = ?");
            int i = 0;
            pstmt.setInt(++i, dateDimension.getYear());
            pstmt.setInt(++i, dateDimension.getSeason());
            pstmt.setInt(++i, dateDimension.getMonth());
            pstmt.setInt(++i, dateDimension.getWeek());
            pstmt.setInt(++i, dateDimension.getDay());
            pstmt.setString(++i, dateDimension.getType());
            pstmt.setDate(++i, new Date(dateDimension.getCalendar().getTime()));
            rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            } else {
                return -1;
            }
        } finally {
            JdbcManager.closeConnection(null, pstmt, rs);
        }
    }

    /**
     * 计算总新增访客数量
     */
    private void calculateTotalUsers(Configuration conf) {
        // 获取数据库连接
        Connection conn = null;
        try {
            conn = JdbcManager.getConnection(conf, GlobalConstants.WAREHOUSE_OF_REPORT);

            long date = TimeUtil.parseString2Long(conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
            // 开始获取当前时间维度
            DateDimension currentDayDimension = DateDimension.buildDate(date, DateEnum.DAY);
            DateDimension currentWeekDimension = DateDimension.buildDate(date, DateEnum.WEEK);
            DateDimension currentMonthDimension = DateDimension.buildDate(date, DateEnum.MONTH);

            // 获取当前时间维度的维度id
            int currentDayDimensionId = getDateDimensionId(conn, currentDayDimension);
            int currentWeekDimensionId = getDateDimensionId(conn, currentWeekDimension);
            int currentMonthDimensionId = getDateDimensionId(conn, currentMonthDimension);

            // 开始获取上一个时间维度的维度信息
            DateDimension preDayDimension = DateDimension
                    .buildDate(date - GlobalConstants.DAY_OF_MILLISECONDS, DateEnum.DAY);
            DateDimension preWeekDimension = DateDimension
                    .buildDate(TimeUtil.getFirstDayOfPreviousWeek(date), DateEnum.WEEK);
            DateDimension preMonthDimension = DateDimension
                    .buildDate(TimeUtil.getFirstDayOfPreviousMonth(date), DateEnum.MONTH);

            // 开始获取上一个时间维度的维度id
            int preDayDimensionId = getDateDimensionId(conn, preDayDimension);
            int preWeekDimensionId = getDateDimensionId(conn, preWeekDimension);
            int preMonthDimensionId = getDateDimensionId(conn, preMonthDimension);

            // 可以开始更新数据
            // 1、更新stats_user表中的总用户值
            this.updateStatsUserTotalUsers(conn, currentDayDimensionId, preDayDimensionId);
            this.updateStatsUserTotalUsers(conn, currentWeekDimensionId, preWeekDimensionId);
            this.updateStatsUserTotalUsers(conn, currentMonthDimensionId, preMonthDimensionId);
            // 2、更新stat_device_browser表中的总用户值
            this.updateStatsDeviceBrowserTotalUsers(conn, currentDayDimensionId, preDayDimensionId);
            this.updateStatsDeviceBrowserTotalUsers(conn, currentWeekDimensionId,
                    preWeekDimensionId);
            this.updateStatsDeviceBrowserTotalUsers(conn, currentMonthDimensionId,
                    preMonthDimensionId);
        } catch (Exception e) {
            throw new RuntimeException("更新总访客数量出现异常", e);
        } finally {
            JdbcManager.closeConnection(conn, null, null);
        }

    }

    /**
     * 处理参数
     * 
     * @param conf
     * @param args
     */
    private void processArgs(Configuration conf, String[] args) {
        String date = null;
        for (int i = 0; i < args.length; i++) {
            if ("-d".equals(args[i])) {
                if (i + 1 < args.length) {
                    date = args[++i];
                    break;
                }
            }
        }

        // 需要默认参数
        if (StringUtils.isBlank(date) || !TimeUtil.isValidateRunningDate(date)) {
            // 默认给定昨天
            date = TimeUtil.getYesterday();
        }
        conf.set(GlobalConstants.RUNNING_DATE_PARAMES, date);
    }

    private void setHBaseInputConfig(Job job) throws IOException {
        FilterList filterList = new FilterList();
        // 只分析launch事件数据
        filterList.addFilter(
                new SingleColumnValueFilter(EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME,
                        Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME),
                        CompareOp.EQUAL, Bytes.toBytes(EventEnum.LAUNCH.alias)));

        // 只获取部分列
        String[] columns = new String[] { EventLogConstants.LOG_COLUMN_NAME_UUID,
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME,
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM,
                EventLogConstants.LOG_COLUMN_NAME_VERSION,
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME,
                EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION,
                EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME // 不管mapper中是否用户event的值，在column中都必须有
        };
        filterList.addFilter(this.getColumnFilter(columns));

        // 设置scan
        long date = TimeUtil.parseString2Long(conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
        long firstDayOfWeek = TimeUtil.getFirstDayOfThisWeek(date);
        long firstDayOfMonth = TimeUtil.getFirstDayOfThisMonth(date);
        long lastDayOfWeek = TimeUtil.getFirstDayOfNextWeek(date);
        long lastDayOfMonth = TimeUtil.getFirstDayOfNextMonth(date);
        long today = TimeUtil.getTodayInMillis() + GlobalConstants.DAY_OF_MILLISECONDS;
        long startDate, endDate;

        // 如果周在月之前，那么开始时间是周，否则是月
        if (firstDayOfWeek < firstDayOfMonth) {
            startDate = firstDayOfWeek;
        } else {
            startDate = firstDayOfMonth;
        }

        if (today > lastDayOfWeek || today > lastDayOfMonth) {
            if (lastDayOfWeek < lastDayOfMonth) {
                endDate = lastDayOfMonth;
            } else {
                endDate = lastDayOfWeek;
            }
        } else {
            endDate = today;
        }

        HBaseAdmin admin = null;
        List<Scan> scans = new ArrayList<Scan>();
        try {
            admin = new HBaseAdmin(conf);

            for (long begin = startDate; begin < endDate;) {
                // hbase表后缀
                String tableNameSuffix = TimeUtil.parseLong2String(begin,
                        TimeUtil.HBASE_TABLE_NAME_SUFFIX_FORMAT);

                byte[] tableName = Bytes
                        .toBytes(EventLogConstants.HBASE_NAME_EVENT_LOGS + tableNameSuffix);
                if (admin.tableExists(tableName)) {
                    // 表存在
                    Scan scan = new Scan();
                    scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, tableName);
                    scan.setFilter(filterList);
                    scans.add(scan);
                }
                begin += GlobalConstants.DAY_OF_MILLISECONDS;
            }
        } catch (Exception e) {
            throw new RuntimeException("创建HBaseAdmin发生异常", e);
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    // nothings
                }
            }
        }

        if (scans.isEmpty()) {
            throw new IOException("没有表存在，无法创建scan集合");
        }
        TableMapReduceUtil.initTableMapperJob(scans, NewInstallUserMapper.class,
                StatsUserDimension.class, Text.class, job, false);

    }

    private Filter getColumnFilter(String[] columns) {
        int length = columns.length;
        byte[][] filter = new byte[length][];
        for (int i = 0; i < length; i++) {
            filter[i] = Bytes.toBytes(columns[i]);
        }
        return new MultipleColumnPrefixFilter(filter);
    }

}
