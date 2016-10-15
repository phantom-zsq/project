package com.phantom.hadoop.hadoopproject.lvmama.mr.stats.en;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Partitioner;

import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.stats.StatsEventDimension;

/**
 * 二次排序相关类
 * 
 * @author ibf
 *
 */
public class EventSecondrySort {
    /**
     * 自定义event分区类<br/>
     * 分区考虑的字段信息必须分组的字段信息一样(不是必须)
     * 
     * @author ibf
     *
     */
    public static class EventPartitioner extends Partitioner<StatsEventDimension, NullWritable> {

        @Override
        public int getPartition(StatsEventDimension key, NullWritable value, int numPartitions) {
            int hashCode = key.getStatsCommon().hashCode();
            hashCode += key.getEvent().getFlowId();
            return (hashCode & Integer.MAX_VALUE) % numPartitions;
        }

    }

    /**
     * 自定义的event排序类<br/>
     * 本身来讲需要实现RawComparator类, 对底层字节比较掌握的比较透彻，性能比实现WritableComparator要好<br/>
     * WritableComparator比较流程：
     *  1、将字节转换为定义的key对象
     *  2、调用compare方法进行比较
     * 
     * @author ibf
     *
     */
    public static class EventSortComparator extends WritableComparator {
        /**
         * 无参构造函数
         */
        public EventSortComparator() {
            // 第一个参数是key的类型，第二个参数的意思是是否进行初始化创建
            super(StatsEventDimension.class, true); // 一定需要的
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            StatsEventDimension key1 = (StatsEventDimension) a;
            StatsEventDimension key2 = (StatsEventDimension) b;

            int tmp = key1.getStatsCommon().compareTo(key2.getStatsCommon());
            if (tmp != 0) {
                return tmp;
            }

            tmp = Integer.compare(key1.getEvent().getFlowId(), key2.getEvent().getFlowId());
            if (tmp != 0) {
                return tmp;
            }

            tmp = key1.getSessionId().compareTo(key2.getSessionId());
            if (tmp != 0) {
                return tmp;
            }

            tmp = Long.compare(key1.getServerTime(), key2.getServerTime());
            if (tmp != 0) {
                return tmp;
            }

            tmp = Integer.compare(key1.getEvent().getSeq(), key2.getEvent().getSeq());
            return tmp;
        }
    }

    /**
     * 自定义的event分组
     * 
     * @author ibf
     *
     */
    public static class EventGroupingComparator extends WritableComparator {
        public EventGroupingComparator() {
            super(StatsEventDimension.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable a, WritableComparable b) {
            StatsEventDimension key1 = (StatsEventDimension) a;
            StatsEventDimension key2 = (StatsEventDimension) b;

            int tmp = key1.getStatsCommon().compareTo(key2.getStatsCommon());
            if (tmp != 0) {
                return tmp;
            }

            tmp = Integer.compare(key1.getEvent().getFlowId(), key2.getEvent().getFlowId());
            return tmp;
        }
    }
}
