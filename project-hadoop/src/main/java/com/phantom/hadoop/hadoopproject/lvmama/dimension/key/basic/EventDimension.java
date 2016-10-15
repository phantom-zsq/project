package com.phantom.hadoop.hadoopproject.lvmama.dimension.key.basic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.BaseDimension;

/**
 * event维度类
 * 
 * @author ibeifeng
 *
 */
public class EventDimension extends BaseDimension {
    // 数据库主键id
    private int id;
    // 浏览器名称
    private String category;
    // 浏览器版本号
    private String action;

    // 来源id
    private int originId;
    // 是否允许多个子事件
    private int hasMultChidren;
    // 事件流id
    private int flowId;
    // 触发顺序号
    private int seq;

    /**
     * 默认构造函数，必须给定
     */
    public EventDimension() {
        super();
    }

    public String getCategory() {
        return category;
    }


    public void setCategory(String category) {
        this.category = category;
    }


    public String getAction() {
        return action;
    }


    public void setAction(String action) {
        this.action = action;
    }


    public int getOriginId() {
        return originId;
    }


    public void setOriginId(int originId) {
        this.originId = originId;
    }


    public int getHasMultChidren() {
        return hasMultChidren;
    }


    public void setHasMultChidren(int hasMultChidren) {
        this.hasMultChidren = hasMultChidren;
    }


    public int getFlowId() {
        return flowId;
    }


    public void setFlowId(int flowId) {
        this.flowId = flowId;
    }


    public int getSeq() {
        return seq;
    }


    public void setSeq(int seq) {
        this.seq = seq;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(id);
        out.writeUTF(this.category);
        out.writeUTF(this.action);
        out.writeInt(this.flowId);
        out.writeInt(this.hasMultChidren);
        out.writeInt(this.seq);
        out.writeInt(this.originId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.category = in.readUTF();
        this.action = in.readUTF();
        this.flowId =in.readInt();
        this.hasMultChidren = in.readInt();
        this.seq = in.readInt();
        this.originId = in.readInt();
    }

    @Override
    public int compareTo(BaseDimension o) {
        // 另外写排序类
        return 0;
    }
}
