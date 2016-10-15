package com.phantom.hadoop.hadoopproject.lvmama.mr;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.phantom.hadoop.hadoopproject.lvmama.common.GlobalConstants;
import com.phantom.hadoop.hadoopproject.lvmama.common.KpiType;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.BaseDimension;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.value.BaseStatsValueWritable;
import com.phantom.hadoop.hadoopproject.lvmama.service.converter.IDimensionConverter;
import com.phantom.hadoop.hadoopproject.lvmama.service.converter.client.DimensionConverterClient;
import com.phantom.hadoop.hadoopproject.lvmama.util.JdbcManager;

/**
 * 
 * 自定义输出到mysql的outputformat类
 * 
 * @author ibf
 *
 */
public class TransformerOutputFormat extends OutputFormat<BaseDimension, BaseStatsValueWritable> {

    @Override
    public RecordWriter<BaseDimension, BaseStatsValueWritable> getRecordWriter(
            TaskAttemptContext context) throws IOException, InterruptedException {
        // 返回一个具体定义如何输出数据的对象, recordwriter被称为数据的输出器
        Configuration conf = context.getConfiguration();
        IDimensionConverter converter = DimensionConverterClient.createDimensionConverter(conf);
        Connection conn = null;

        try {
            conn = JdbcManager.getConnection(conf, GlobalConstants.WAREHOUSE_OF_REPORT);
            conn.setAutoCommit(false); // 关闭自动提交机制
        } catch (Exception e) {
            throw new RuntimeException("获取数据库连接失败", e);
        }
        return new TransformerRecordWriter(conn, conf, converter);
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        // 这个方法在自己实现的时候不需要关注，如果你非要关注，最多检查一下表数据存在
    }

    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new FileOutputCommitter(FileOutputFormat.getOutputPath(context), context);
    }

    /**
     * 自定义的数据输出器
     * 
     * @author ibf
     *
     */
    public class TransformerRecordWriter
            extends RecordWriter<BaseDimension, BaseStatsValueWritable> {
        private Connection conn = null;
        private Configuration conf = null;
        private IDimensionConverter converter = null;
        private Map<KpiType, PreparedStatement> map = new HashMap<>();
        private Map<KpiType, Integer> batch = new HashMap<>();

        public TransformerRecordWriter(Connection conn, Configuration conf,
                IDimensionConverter converter) {
            super();
            this.conn = conn;
            this.conf = conf;
            this.converter = converter;
        }

        @Override
        public void write(BaseDimension key, BaseStatsValueWritable value)
                throws IOException, InterruptedException {
            // 输出数据, 当在reduce中调用context.write方法的时候，底层调用的是该方法
            KpiType kpiType = value.getKpi();

            String sql = this.conf.get(kpiType.name);
            PreparedStatement pstmt = null;
            int count = 1;
            try {
                if (map.get(kpiType) == null) {
                    // 第一次创建
                    pstmt = this.conn.prepareStatement(sql);
                    map.put(kpiType, pstmt);
                } else {
                    // 标示以及存在
                    pstmt = map.get(kpiType);
                    if (!batch.containsKey(kpiType)) {
                        batch.put(kpiType, count);
                    }
                    count = batch.get(kpiType);
                    count++;
                }
                batch.put(kpiType, count);

                // 针对特定的MR任务有特定的输出器:IOutputCollector
                String collectorClassName = conf
                        .get(GlobalConstants.OUTPUT_COLLECTOR_KEY_PREFIX + kpiType.name);
                Class<?> clazz = Class.forName(collectorClassName);
                // 创建对象, 要求实现子类一定要有一个无参数的构造方法
                IOutputCollector collector = (IOutputCollector) clazz.newInstance();
                collector.collect(conf, key, value, pstmt, converter);

                // 批量提交
                if (count % conf.getInt(GlobalConstants.JDBC_BATCH_NUMBER,
                        GlobalConstants.DEFAULT_JDBC_BATCH_NUMBER) == 0) {
                    pstmt.executeBatch(); // 批量提交
                    conn.commit();
                    batch.remove(kpiType); // 移除以及存在的输出数据
                }
            } catch (Exception e) {
                throw new IOException("数据输出产生异常", e);
            }
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            // 关闭资源使用，最终一定会调用
            try {

                try {
                    for (Map.Entry<KpiType, PreparedStatement> entry : this.map.entrySet()) {
                        entry.getValue().executeBatch();
                    }
                } catch (Exception e) {
                    throw new IOException("输出数据出现异常", e);
                } finally {
                    try {
                        if (conn != null) {
                            conn.commit();
                        }
                    } catch (Exception e) {
                        // nothings
                    } finally {
                        if (conn != null) {
                            for (Map.Entry<KpiType, PreparedStatement> entry : this.map
                                    .entrySet()) {
                                try {
                                    entry.getValue().close();
                                } catch (SQLException e) {
                                    // nothings
                                }
                            }
                            try {
                                conn.close();
                            } catch (SQLException e) {
                                // nothings
                            }
                        }
                    }
                }
            } finally {
                // 关闭远程连接
                DimensionConverterClient.stopDimensionConverterProxy(converter);
            }
        }

    }

}
