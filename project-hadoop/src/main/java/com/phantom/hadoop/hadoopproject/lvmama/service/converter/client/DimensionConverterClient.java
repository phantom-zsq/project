package com.phantom.hadoop.hadoopproject.lvmama.service.converter.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.ipc.RPC;

import com.phantom.hadoop.hadoopproject.lvmama.dimension.key.BaseDimension;
import com.phantom.hadoop.hadoopproject.lvmama.service.converter.IDimensionConverter;
import com.phantom.hadoop.hadoopproject.lvmama.service.converter.server.DimensionConverterImpl;

/**
 * 操作dimensionConverter相关服务的client端工具类
 * 
 * @author ibf
 *
 */
public class DimensionConverterClient {
    /**
     * 创建连接对象
     * 
     * @param conf
     * @return
     * @throws IOException
     */
    public static IDimensionConverter createDimensionConverter(Configuration conf)
            throws IOException {
        // 创建操作
        String[] cf = fetchDimensionConverterConfiguration(conf);
        String address = cf[0]; // 获取ip地址
        int port = Integer.valueOf(cf[1]); // 获取端口号
        // 创建代理对象
        return new InnerDimensionConverterProxy(conf, address, port);
    }

    /**
     * 关闭客户端连接
     * 
     * @param proxy
     */
    public static void stopDimensionConverterProxy(IDimensionConverter proxy) {
        if (proxy != null) {
            InnerDimensionConverterProxy innerProxy = (InnerDimensionConverterProxy) proxy;
            RPC.stopProxy(innerProxy.proxy);
        }
    }

    /**
     * 读取配置信息，ip地址和端口号
     * 
     * @author ibf
     * @throws IOException
     *
     */
    private static String[] fetchDimensionConverterConfiguration(Configuration conf)
            throws IOException {
        FileSystem fs = null;
        BufferedReader br = null;
        try {
            fs = FileSystem.get(conf);
            br = new BufferedReader(
                    new InputStreamReader(fs.open(new Path(IDimensionConverter.CONFIG_SAVE_PATH))));
            String[] result = new String[2];
            result[0] = br.readLine().trim(); // ip地址
            result[1] = br.readLine().trim(); // 端口号
            return result;
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (Exception ee) {
                    // nothing
                }
            }
            // TODO: 默认配置参数的情况下，这里不要调用fs.close()方法，因为可能fs这个对象在多个线程中公用
        }
    }

    /**
     * 内部代理类<br/>
     * 增加缓存在本地磁盘的功能
     * 
     * @author ibf
     *
     */
    private static class InnerDimensionConverterProxy implements IDimensionConverter {
        /**
         * 远程连接代理对象
         */
        private IDimensionConverter proxy = null;
        /**
         * 本地缓存对象，最多缓存1000条记录
         */
        private Map<String, Integer> cache = new LinkedHashMap<String, Integer>() {
            private static final long serialVersionUID = -731083744087467205L;

            @Override
            protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
                return this.size() > 1000;
            }
        };

        /**
         * 构造函数，创建代理对象
         * 
         * @param conf
         * @param address
         * @param port
         * @throws IOException
         */
        public InnerDimensionConverterProxy(Configuration conf, String address, int port)
                throws IOException {
            this.proxy = RPC.getProxy(IDimensionConverter.class, IDimensionConverter.versionID,
                    new InetSocketAddress(address, port), conf);
        }

        @Override
        public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
            return this.proxy.getProtocolVersion(protocol, clientVersion);
        }

        @Override
        public ProtocolSignature getProtocolSignature(String protocol, long clientVersion,
                int clientMethodsHash) throws IOException {
            return this.proxy.getProtocolSignature(protocol, clientVersion, clientMethodsHash);
        }

        @Override
        public int getDimensionIdByValue(BaseDimension dimension) throws IOException {
            /**
             * 创建cache的key值
             */
            String key = DimensionConverterImpl.buildCacheKey(dimension);
            Integer value = this.cache.get(key);
            if (value == null) {
                // 通过proxy获取数据
                value = this.proxy.getDimensionIdByValue(dimension);
                this.cache.put(key, value);
            }
            return value;
        }

    }
}
