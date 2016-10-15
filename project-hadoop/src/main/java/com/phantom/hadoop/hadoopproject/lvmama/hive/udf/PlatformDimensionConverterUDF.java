package com.phantom.hadoop.hadoopproject.lvmama.hive.udf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.basic.PlatformDimension;
import com.phantom.hadoop.hadoopproject.lvmama.service.converter.IDimensionConverter;
import com.phantom.hadoop.hadoopproject.lvmama.service.converter.client.DimensionConverterClient;

public class PlatformDimensionConverterUDF extends UDF {
    private IDimensionConverter converter = null;

    public PlatformDimensionConverterUDF() throws IOException {
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
     * 根据给定的平台维度和版本号返回具体的维度id值
     * 
     * @param platform
     * @param version
     * @return
     * @throws IOException
     */
    public IntWritable evaluate(Text platform, Text version) throws IOException {
        PlatformDimension platformDimension = new PlatformDimension(platform.toString(),
                version.toString());
        return new IntWritable(this.converter.getDimensionIdByValue(platformDimension));
    }
}
