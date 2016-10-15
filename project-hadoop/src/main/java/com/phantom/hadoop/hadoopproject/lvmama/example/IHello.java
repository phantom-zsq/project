package com.phantom.hadoop.hadoopproject.lvmama.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;

/**
 * 如果需要实现hadoop rpc，必须继承VersionedProtocols接口
 * 
 * @author ibf
 *
 */
public interface IHello extends VersionedProtocol {
    // 必须有一个versionID的属性，该属性不能缺少，而且名称也不能改动
    // 该属性的主要作用是控制客户端和服务器的版本
    public static final long versionID = 1l;

    public String sayString();

    public Text sayText();

    public String sayStringWithPerson(String name);

    public Text sayTextWithPerson(Text name);
}
