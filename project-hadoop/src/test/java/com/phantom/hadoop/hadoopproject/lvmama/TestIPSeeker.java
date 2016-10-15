package com.phantom.hadoop.hadoopproject.lvmama;

import com.phantom.hadoop.hadoopproject.lvmama.util.IPSeekerExt;
import com.phantom.hadoop.hadoopproject.lvmama.util.IPSeekerExt.RegionInfo;

public class TestIPSeeker {
    public static void main(String[] args) {
        String ip = "114.92.217.149";
        test1(ip);
    }

    public static void test1(String ip) {
        RegionInfo info = IPSeekerExt.getInstance().analysisIp(ip);
        System.out.println(info);
    }

    public static void test2() {
        // 获取所有ip
        IPSeekerExt.getInstance().getAllIp();
        // 针对上述ip集合，所有的中国省份自治区进行找规律
    }
}
