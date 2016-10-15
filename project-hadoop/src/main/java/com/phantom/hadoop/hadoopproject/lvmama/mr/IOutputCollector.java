package com.phantom.hadoop.hadoopproject.lvmama.mr;

import java.io.IOException;
import java.sql.PreparedStatement;

import org.apache.hadoop.conf.Configuration;

import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.BaseDimension;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.value.BaseStatsValueWritable;
import com.phantom.hadoop.hadoopproject.lvmama.service.converter.IDimensionConverter;

/**
 * 定义具体mapreduce对于的输出操作代码
 * 
 * @author ibf
 *
 */
public interface IOutputCollector {

    /**
     * 定义具体执行sql数据插入的方法
     * 
     * @param conf
     * @param key
     * @param value
     * @param pstmt
     * @param convertere
     * @throws IOException
     */
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value,
            PreparedStatement pstmt, IDimensionConverter convertere) throws IOException;
}
