package com.phantom.hadoop.hadoopproject.lvmama.example.client;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;

import com.phantom.hadoop.hadoopproject.lvmama.example.IHello;

/**
 * 远程访问（两个不同jvm之间的访问）
 * @author ibf
 *
 */
public class HelloClient {
    public static void main(String[] args) throws IOException {
        String address = "127.0.0.1";
        int port = 9999;
        IHello hello = RPC.getProxy(IHello.class, IHello.versionID,
                new InetSocketAddress(address, port), new Configuration());
        System.out.println("创建成功....");
        System.out.println("client: " + hello.sayString());
        System.out.println("client: " + hello.sayStringWithPerson("北风网"));
        System.out.println("client: " + hello.sayText());
        System.out.println("client: " + hello.sayTextWithPerson(new Text("大数据")));

        // 关闭操作，一定记得
        RPC.stopProxy(hello);
    }
}
