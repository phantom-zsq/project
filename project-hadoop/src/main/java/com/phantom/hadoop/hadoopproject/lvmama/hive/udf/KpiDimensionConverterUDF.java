package com.phantom.hadoop.hadoopproject.lvmama.hive.udf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.basic.KpiDimension;
import com.phantom.hadoop.hadoopproject.lvmama.service.converter.IDimensionConverter;
import com.phantom.hadoop.hadoopproject.lvmama.service.converter.client.DimensionConverterClient;

public class KpiDimensionConverterUDF extends UDF {
    private IDimensionConverter converter = null;

    public KpiDimensionConverterUDF() throws IOException {
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
     * 根据kpi的名称获取对应的维度id
     * 
     * @param kpiName
     * @return
     * @throws IOException
     */
    public IntWritable evaluate(Text kpiName) throws IOException {
        KpiDimension kpiDimension = new KpiDimension(kpiName.toString());
        return new IntWritable(this.converter.getDimensionIdByValue(kpiDimension));
    }
}
