package com.phantom.hadoop.hadoopproject.lvmama.mr.stats.en;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import com.phantom.hadoop.hadoopproject.lvmama.common.GlobalConstants;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.BaseDimension;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.stats.StatsEventDimension;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.value.BaseStatsValueWritable;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.value.MapWritableValue;
import com.phantom.hadoop.hadoopproject.lvmama.mr.IOutputCollector;
import com.phantom.hadoop.hadoopproject.lvmama.service.converter.IDimensionConverter;

public class EventCollector implements IOutputCollector {

    @Override
    public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value,
            PreparedStatement pstmt, IDimensionConverter convertere) throws IOException {
        StatsEventDimension statsEventDimension = (StatsEventDimension) key;
        MapWritableValue mapValue = (MapWritableValue) value;
        int times = ((IntWritable) (mapValue.getValue().get(new IntWritable(-1)))).get();

        int i = 0;
        try {
            pstmt.setInt(++i, convertere
                    .getDimensionIdByValue(statsEventDimension.getStatsCommon().getPlatform()));
            pstmt.setInt(++i, convertere
                    .getDimensionIdByValue(statsEventDimension.getStatsCommon().getDate()));
            pstmt.setInt(++i, statsEventDimension.getEvent().getId());
            pstmt.setInt(++i, times);
            pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
            pstmt.setInt(++i, times);

            pstmt.addBatch(); // 最终需要批量执行的
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

}
