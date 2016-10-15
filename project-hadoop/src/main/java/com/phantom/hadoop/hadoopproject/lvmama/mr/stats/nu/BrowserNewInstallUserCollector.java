package com.phantom.hadoop.hadoopproject.lvmama.mr.stats.nu;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import com.phantom.hadoop.hadoopproject.lvmama.common.GlobalConstants;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.BaseDimension;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.stats.StatsUserDimension;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.value.BaseStatsValueWritable;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.value.MapWritableValue;
import com.phantom.hadoop.hadoopproject.lvmama.mr.IOutputCollector;
import com.phantom.hadoop.hadoopproject.lvmama.service.converter.IDimensionConverter;

public class BrowserNewInstallUserCollector implements IOutputCollector {

    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value,
            PreparedStatement pstmt, IDimensionConverter convertere) throws IOException {
        StatsUserDimension statsUserDimension = (StatsUserDimension) key;
        MapWritableValue mapWritableValue = (MapWritableValue) value;
        IntWritable newInstallUsers = (IntWritable) mapWritableValue.getValue()
                .get(new IntWritable(-1));

        int i = 0;
        try {
            pstmt.setInt(++i, convertere
                    .getDimensionIdByValue(statsUserDimension.getStatsCommon().getPlatform()));
            pstmt.setInt(++i, convertere
                    .getDimensionIdByValue(statsUserDimension.getStatsCommon().getDate()));
            pstmt.setInt(++i, convertere.getDimensionIdByValue(statsUserDimension.getBrowser()));
            pstmt.setInt(++i, newInstallUsers.get());
            pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
            pstmt.setInt(++i, newInstallUsers.get());

            // 批量执行
            pstmt.addBatch();
        } catch (SQLException e) {
            throw new IOException("sql异常", e);
        }
    }

}
