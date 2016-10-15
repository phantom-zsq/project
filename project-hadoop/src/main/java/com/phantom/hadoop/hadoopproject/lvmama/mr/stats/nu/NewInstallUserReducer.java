package com.phantom.hadoop.hadoopproject.lvmama.mr.stats.nu;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.phantom.hadoop.hadoopproject.lvmama.common.KpiType;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.stats.StatsUserDimension;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.value.MapWritableValue;

public class NewInstallUserReducer extends Reducer<StatsUserDimension, Text, StatsUserDimension, MapWritableValue> {
    private MapWritableValue outputValue = new MapWritableValue();
    private Set<String> unique = new HashSet<String>();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException {
        this.unique.clear();
        for (Text value : values) {
            this.unique.add(value.toString());
        }

        // 设置值
        MapWritable map = new MapWritable();
        map.put(new IntWritable(-1), new IntWritable(this.unique.size())); // 设置活跃访客数量
        this.outputValue.setValue(map);

        // 设置kpi
        String kpiName = key.getStatsCommon().getKpi().getKpiName();
        if (KpiType.BROWSER_NEW_INSTALL_USER.name.equals(kpiName)) {
            // 表示kpi是计算browser相关的数据
            this.outputValue.setKpi(KpiType.BROWSER_NEW_INSTALL_USER);
        } else if (KpiType.NEW_INSTALL_USER.name.equals(kpiName)) {
            // 表示kpi是new install user相关的数据
            this.outputValue.setKpi(KpiType.NEW_INSTALL_USER);
        }

        // 数据输出
        context.write(key, outputValue);
    }
}
