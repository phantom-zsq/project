package com.phantom.hadoop.hadoopproject.lvmama.example.server;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.ProtocolSignature;

import com.phantom.hadoop.hadoopproject.lvmama.example.IHello;

/**
 * 服务器端对于接口的实现类
 * 
 * @author ibf
 *
 */
public class HelloServerImpl implements IHello {

    @Override
    public String sayString() {
        // 参数： java.io.Serializable
        // 参数： Writable
        System.out.println("我是服务器!!");
        return "您好，返回类型为String";
    }

    @Override
    public Text sayText() {
        System.out.println("我是服务器!!");
        return new Text("您好，返回类型为Text");
    }

    @Override
    public String sayStringWithPerson(String name) {
        System.out.println("我是服务器， 接收到的值是: " + name + ", 类型为String.");
        return "你好:" + name + ", 返回类型为String";
    }

    @Override
    public Text sayTextWithPerson(Text name) {
        System.out.println("我是服务器， 接收到的值是: " + name + ", 类型为Text.");
        return new Text("你好:" + name + ", 返回类型为Text");
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
        return IHello.versionID;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol, long clientVersion,
            int clientMethodsHash) throws IOException {
        // 可以为空
        return null;
    }
}
