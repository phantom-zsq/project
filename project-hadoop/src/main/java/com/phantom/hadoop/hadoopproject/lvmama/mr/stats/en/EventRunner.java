package com.phantom.hadoop.hadoopproject.lvmama.mr.stats.en;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.phantom.hadoop.hadoopproject.lvmama.common.EventLogConstants;
import com.phantom.hadoop.hadoopproject.lvmama.common.EventLogConstants.EventEnum;
import com.phantom.hadoop.hadoopproject.lvmama.common.GlobalConstants;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.stats.StatsEventDimension;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.value.MapWritableValue;
import com.phantom.hadoop.hadoopproject.lvmama.mr.TransformerOutputFormat;
import com.phantom.hadoop.hadoopproject.lvmama.mr.stats.en.EventSecondrySort.EventGroupingComparator;
import com.phantom.hadoop.hadoopproject.lvmama.mr.stats.en.EventSecondrySort.EventPartitioner;
import com.phantom.hadoop.hadoopproject.lvmama.mr.stats.en.EventSecondrySort.EventSortComparator;
import com.phantom.hadoop.hadoopproject.lvmama.util.TimeUtil;

public class EventRunner implements Tool {
    private Configuration conf = null;

    public static void main(String[] args) {
        try {
            int exitCode = ToolRunner.run(new EventRunner(), args);
            System.out.println(exitCode);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void setConf(Configuration that) {
        that.addResource("output-collector.xml");
        that.addResource("query-mapping.xml");
        that.addResource("transformer-env.xml");
        this.conf = HBaseConfiguration.create(that);
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        // 参数处理
        this.processArgs(conf, args);

        // 开始创建job
        Job job = Job.getInstance(conf, "event-times");
        // 设置job相关参数
        job.setJarByClass(EventRunner.class);
        // 输入信息指定，hbase表指定
        this.setHBaseInputConfig(job);
        // 指定reducer
        job.setReducerClass(EventReducer.class);
        job.setOutputKeyClass(StatsEventDimension.class);
        job.setOutputValueClass(MapWritableValue.class);

        // 二次排序的类指定
        job.setPartitionerClass(EventPartitioner.class);
        job.setSortComparatorClass(EventSortComparator.class);
        job.setGroupingComparatorClass(EventGroupingComparator.class);

        // outputformat指定
        job.setOutputFormatClass(TransformerOutputFormat.class);
        return job.waitForCompletion(true) ? 0 : -1;
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
                        CompareOp.EQUAL, Bytes.toBytes(EventEnum.EVENT.alias)));

        // 只获取部分列
        String[] columns = new String[] { EventLogConstants.LOG_COLUMN_NAME_EVENT_CATEGORY,
                EventLogConstants.LOG_COLUMN_NAME_EVENT_ACTION,
                EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME,
                EventLogConstants.LOG_COLUMN_NAME_PLATFORM,
                EventLogConstants.LOG_COLUMN_NAME_VERSION,
                EventLogConstants.LOG_COLUMN_NAME_SESSION_ID,
                EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME // 不管mapper中是否用户event的值，在column中都必须有
        };
        filterList.addFilter(this.getColumnFilter(columns));

        // 设置scan
        long date = TimeUtil.parseString2Long(conf.get(GlobalConstants.RUNNING_DATE_PARAMES));

        HBaseAdmin admin = null;
        List<Scan> scans = new ArrayList<Scan>();
        try {
            admin = new HBaseAdmin(conf);

            // hbase表后缀
            String tableNameSuffix = TimeUtil.parseLong2String(date,
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
        TableMapReduceUtil.initTableMapperJob(scans, EventMapper.class, StatsEventDimension.class,
                NullWritable.class, job, false);

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
