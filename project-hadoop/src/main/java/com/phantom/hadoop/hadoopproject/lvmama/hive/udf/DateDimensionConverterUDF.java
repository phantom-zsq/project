package com.phantom.hadoop.hadoopproject.lvmama.hive.udf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import com.phantom.hadoop.hadoopproject.lvmama.common.DateEnum;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.basic.DateDimension;
import com.phantom.hadoop.hadoopproject.lvmama.service.converter.IDimensionConverter;
import com.phantom.hadoop.hadoopproject.lvmama.service.converter.client.DimensionConverterClient;

public class DateDimensionConverterUDF extends UDF {
    private IDimensionConverter converter = null;

    public DateDimensionConverterUDF() throws IOException {
        Configuration conf = new Configuration();
        conf.addResource("output-collector.xml");
        conf.addResource("query-mapping.xml");
        conf.addResource("transformer-env.xml");
        this.converter = DimensionConverterClient.createDimensionConverter(conf);

        // 添加一个当jvm关闭的时候调用的线程
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

            @Override
            public void run() {
                DimensionConverterClient.stopDimensionConverterProxy(converter);
            }
        }));
    }

    /**
     * 根据给定的时间戳字符串和需要的时间维度类型返回维度id
     * 
     * @param date
     * @param type
     * @return
     * @throws IOException
     */
    public IntWritable evaluate(Text date, Text type) throws IOException {
        DateDimension dateDimension = DateDimension.buildDate(Long.valueOf(date.toString()),
                DateEnum.valueOfName(type.toString()));
        return new IntWritable(this.converter.getDimensionIdByValue(dateDimension));
    }

    /**
     * 根据给定的时间戳数字和需要的时间维度类型返回维度id
     * 
     * @param date
     * @param type
     * @return
     * @throws IOException
     */
    public IntWritable evaluate(LongWritable date, Text type) throws IOException {
        DateDimension dateDimension = DateDimension.buildDate(date.get(),
                DateEnum.valueOfName(type.toString()));
        return new IntWritable(this.converter.getDimensionIdByValue(dateDimension));
    }
}
