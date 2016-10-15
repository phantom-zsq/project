package com.phantom.hadoop.hadoopproject.lvmama.service.converter;

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;

import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.BaseDimension;

/**
 * 提供专门操作dimension表的接口
 * 
 * @author ibf
 *
 */
public interface IDimensionConverter extends VersionedProtocol {
    // 版本id
    public static final long versionID = 1;
    // 保存在hdfs上的文件
    public static final String CONFIG_SAVE_PATH = "/beifeng/transformer/rpc/config";

    /**
     * 根据dimension的value值获取id<br/>
     * 如果数据库中有，那么直接返回。如果没有，那么进行插入后返回新的id值
     * 
     * @param dimension
     * @return
     * @throws IOException
     */
    public int getDimensionIdByValue(BaseDimension dimension) throws IOException;
}
