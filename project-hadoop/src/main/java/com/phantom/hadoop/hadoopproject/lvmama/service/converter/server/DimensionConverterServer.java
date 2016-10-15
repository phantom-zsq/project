package com.phantom.hadoop.hadoopproject.lvmama.service.converter.server;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.security.PrivilegedAction;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.Server;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import com.phantom.hadoop.hadoopproject.lvmama.service.converter.IDimensionConverter;

/**
 * IDimensionConverter服务接口的启动类
 * 
 * @author ibf
 *
 */
public class DimensionConverterServer {
    private static final Logger logger = Logger.getLogger(DimensionConverterServer.class);
    private AtomicBoolean isRunning = new AtomicBoolean(false); // 标识是否启动
    private Server server = null;// 服务对象
    private Configuration conf = null;

    public static void main(String[] args) {
        UserGroupInformation.createRemoteUser("ibeifeng").doAs(new PrivilegedAction<Object>() {

            @Override
            public Object run() {
                Configuration conf = new Configuration();
                DimensionConverterServer dcs = new DimensionConverterServer(conf);
                dcs.startServer();
                return null;
            }

        });
    }

    public DimensionConverterServer(Configuration conf) {
        this.conf = conf;

        // 添加一个钩子，进行关闭操作
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    DimensionConverterServer.this.stopServer();
                } catch (IOException e) {
                    // nothing
                }
            }
        }));
    }

    /**
     * 关闭服务
     * 
     * @throws IOException
     */
    public void stopServer() throws IOException {
        logger.info("关闭服务开始");
        try {
            this.removeListenerAddress();
        } finally {
            if (this.server != null) {
                Server tmp = this.server;
                this.server = null;
                tmp.stop();
            }
        }
        logger.info("关闭服务结束");
    }

    /**
     * 启动服务
     * 
     */
    public void startServer() {
        logger.info("开始启动服务");
        synchronized (this) {
            if (isRunning.get()) {
                // 启动完成
                return;
            }

            try {
                // 创建一个对象
                IDimensionConverter converter = new DimensionConverterImpl();
                // 创建服务
                this.server = new RPC.Builder(conf).setInstance(converter)
                        .setProtocol(IDimensionConverter.class).setVerbose(true).build();
                // 获取ip地址和端口号
                int port = this.server.getPort();
                String address = InetAddress.getLocalHost().getHostAddress();
                this.saveListenerAddress(address, port);
                // 启动
                this.server.start();
                // 标识成功
                isRunning.set(true);
                logger.info("启动服务成功，监听ip地址:" + address + "，端口:" + port);
            } catch (Throwable e) {
                isRunning.set(false);
                logger.error("启动服务发生异常", e);
                // 关闭可能异常创建的服务
                try {
                    this.stopServer();
                } catch (Throwable ee) {
                    // nothing
                }
                throw new RuntimeException("启动服务发生异常", e);
            }
        }

    }

    /**
     * 保存监听信息
     * 
     * @param address
     * @param port
     * @throws IOException
     */
    private void saveListenerAddress(String address, int port) throws IOException {
        // 删除已经存在的
        this.removeListenerAddress();

        // 进行数据输出操作
        FileSystem fs = null;
        BufferedWriter bw = null;

        try {
            fs = FileSystem.get(conf);
            Path path = new Path(IDimensionConverter.CONFIG_SAVE_PATH);
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(path)));
            bw.write(address);
            bw.newLine();
            bw.write(String.valueOf(port));
        } finally {
            if (bw != null) {
                try {
                    bw.close();
                } catch (IOException e) {
                    // nothing
                }
            }

            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    // nothing
                }
            }
        }
    }

    /**
     * 删除监听信息
     * 
     * @throws IOException
     */
    private void removeListenerAddress() throws IOException {
        FileSystem fs = null;

        try {
            fs = FileSystem.get(conf);
            Path path = new Path(IDimensionConverter.CONFIG_SAVE_PATH);
            if (fs.exists(path)) {
                // 存在，则删除
                fs.delete(path, true);
            }
        } finally {

            if (fs != null) {
                try {
                    fs.close();
                } catch (IOException e) {
                    // nothing
                }
            }
        }
    }
}
