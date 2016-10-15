package com.phantom.hadoop.hadoopproject.lvmama.mr.stats.en;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;

import com.phantom.hadoop.hadoopproject.lvmama.common.DateEnum;
import com.phantom.hadoop.hadoopproject.lvmama.common.EventLogConstants;
import com.phantom.hadoop.hadoopproject.lvmama.common.GlobalConstants;
import com.phantom.hadoop.hadoopproject.lvmama.common.KpiType;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.basic.DateDimension;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.basic.EventDimension;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.basic.KpiDimension;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.basic.PlatformDimension;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.stats.StatsCommonDimension;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.stats.StatsEventDimension;
import com.phantom.hadoop.hadoopproject.lvmama.util.JdbcManager;

/**
 * mapper类
 * 
 * @author ibf
 *
 */
public class EventMapper extends TableMapper<StatsEventDimension, NullWritable> {
    private static final Logger logger = Logger.getLogger(EventMapper.class);
    private List<EventDimension> eventDimensions = new ArrayList<EventDimension>();
    private byte[] family = EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME;
    private StatsEventDimension statsEventDimension = new StatsEventDimension();
    private StatsCommonDimension statsCommonDimension = this.statsEventDimension.getStatsCommon();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // event数据初始化，这部分代码可以放到runner中执行，将结果通过DistributedCache来进行缓存。
        Configuration conf = context.getConfiguration();
        Connection conn = null;
        try {
            conn = JdbcManager.getConnection(conf, GlobalConstants.WAREHOUSE_OF_REPORT);
            initEventDimensions(conn);
        } catch (SQLException e) {
            logger.error("初始化异常", e);
            throw new IOException(e);
        } finally {
            // 关闭连接
            JdbcManager.closeConnection(conn, null, null);
        }

        // 初始化kpi
        this.statsCommonDimension.setKpi(new KpiDimension(KpiType.EVENT_TIMES.name));
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context)
            throws IOException, InterruptedException {
        String platform = Bytes.toString(
                value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_PLATFORM)));
        String version = Bytes.toString(
                value.getValue(family, Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_VERSION)));
        String sessionId = Bytes.toString(value.getValue(family,
                Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SESSION_ID)));
        String serverTime = Bytes.toString(value.getValue(family,
                Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME)));
        String category = Bytes.toString(value.getValue(family,
                Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_CATEGORY)));
        String action = Bytes.toString(value.getValue(family,
                Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_ACTION)));

        if (StringUtils.isBlank(serverTime) || StringUtils.isBlank(sessionId)) {
            logger.debug("数据格式不正确，过滤该数据!");
            return;
        }

        EventDimension eventDimension = this.fetchEventDimensionByCategoryAndAction(category,
                action);
        if (eventDimension != null) {
            // 数据正常，继续处理
            long longOfServerTime = Long.valueOf(serverTime.trim());
            // 时间维度
            // TODO: 过滤不是当天的数据
            DateDimension dayDimension = DateDimension.buildDate(longOfServerTime, DateEnum.DAY);
            // 平台维度
            List<PlatformDimension> platformDimensions = PlatformDimension.buildList(platform,
                    version);

            // 输出
            statsCommonDimension.setDate(dayDimension);
            this.statsEventDimension.setServerTime(longOfServerTime);
            this.statsEventDimension.setSessionId(sessionId);
            this.statsEventDimension.setEvent(eventDimension);
            for (PlatformDimension pf : platformDimensions) {
                statsCommonDimension.setPlatform(pf);
                context.write(statsEventDimension, NullWritable.get());
            }
        } else {
            logger.error("数据库中没有找到对于的category和action值:" + category + "; " + action);
            return;
        }
    }

    /**
     * 初始化维度信息
     * 
     * @param conn
     * @throws SQLException
     */
    private void initEventDimensions(Connection conn) throws SQLException {
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        String sql = "select * from dimension_event";
        try {
            pstmt = conn.prepareStatement(sql);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                EventDimension event = new EventDimension();
                event.setFlowId(rs.getInt("flow_id"));
                event.setAction(rs.getString("action"));
                event.setCategory(rs.getString("category"));
                event.setHasMultChidren(rs.getInt("has_multi_children"));
                event.setOriginId(rs.getInt("origin_id"));
                event.setSeq(rs.getInt("seq"));
                event.setId(rs.getInt("id"));
                eventDimensions.add(event);
            }
        } finally {
            JdbcManager.closeConnection(null, pstmt, rs);
        }

    }

    /**
     * 从缓存中获取对于的事件维度数据，如果事件维度数据不存在，返回null
     * 
     * @param category
     * @param action
     * @return
     */
    private EventDimension fetchEventDimensionByCategoryAndAction(String category, String action) {
        for (EventDimension event : this.eventDimensions) {
            if (event.getCategory().equals(category) && event.getAction().equals(action)) {
                return event;
            }
        }
        return null;
    }
}
