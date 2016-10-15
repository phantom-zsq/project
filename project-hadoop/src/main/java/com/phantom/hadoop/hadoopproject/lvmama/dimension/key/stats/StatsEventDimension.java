package com.phantom.hadoop.hadoopproject.lvmama.dimension.key.stats;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.BaseDimension;
import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.basic.EventDimension;

/**
 * 进行事件分析定义的组合维度
 * 
 * @author ibeifeng
 *
 */
public class StatsEventDimension extends StatsDimension {
    /**
     * 公用维度对象
     */
    private StatsCommonDimension statsCommon = new StatsCommonDimension();
    /**
     * 事件维度
     */
    private EventDimension event = new EventDimension();
    private long serverTime;
    private String sessionId;

    public long getServerTime() {
        return serverTime;
    }

    public void setServerTime(long serverTime) {
        this.serverTime = serverTime;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    /**
     * 无参构造方法，必须给定
     */
    public StatsEventDimension() {
        super();
    }

    public StatsCommonDimension getStatsCommon() {
        return statsCommon;
    }

    public void setStatsCommon(StatsCommonDimension statsCommon) {
        this.statsCommon = statsCommon;
    }

    public EventDimension getEvent() {
        return event;
    }

    public void setEvent(EventDimension event) {
        this.event = event;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.statsCommon.write(out);
        this.event.write(out);
        out.writeUTF(this.sessionId);
        out.writeLong(this.serverTime);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.statsCommon.readFields(in);
        this.event.readFields(in);
        this.sessionId = in.readUTF();
        this.serverTime = in.readLong();
    }

    @Override
    public int compareTo(BaseDimension o) {
        return 0;
    }
}
