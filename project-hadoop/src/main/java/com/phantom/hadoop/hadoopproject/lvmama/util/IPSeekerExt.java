package com.phantom.hadoop.hadoopproject.lvmama.util;

import com.phantom.hadoop.hadoopproject.lvmama.common.GlobalConstants;
import com.phantom.hadoop.hadoopproject.lvmama.util.ip.IPSeeker;

/**
 * Ip解析工具类
 * 
 * @author ibf
 *
 */
public class IPSeekerExt extends IPSeeker {
    private static final String ipFilePath = "ip/qqwry.dat";
    private static IPSeekerExt obj = new IPSeekerExt(ipFilePath);

    private IPSeekerExt(String ipFilePath) {
        super(ipFilePath);
    }

    /**
     * 获取ip解析对象
     * 
     * @return
     */
    public static IPSeekerExt getInstance() {
        return obj;
    }

    /**
     * 解析ip地址，如果解析失败，返回null(出现异常的时候才算失败)
     * 
     * @param ip
     * @return
     */
    public RegionInfo analysisIp(String ip) {
        RegionInfo info = new RegionInfo();
        // 判断参数是否为空
        if (ip != null && !"".equals(ip.trim())) {
            // ip不为空
            String country = super.getCountry(ip);
            if (country == null || country.isEmpty()) {
                // 数据库中没有找到ip，直接返回unknown
                return info;
            }

            if (!ERROR_RESULT.equals(country)) {
                // 能够正常解析出国家名称
                if ("局域网".equals(country) || country.trim().endsWith("CZ88")) {
                    // 都可以认为是本地, 没有找到ip返回的值是空
                    info.setCountry("中国");
                    info.setProvince("上海市");
                } else {
                    int length = country.length();
                    int index = country.indexOf("省");
                    if (index > 0) {
                        // 表示是国家的某个省份
                        info.setCountry("中国");
                        info.setProvince(country.substring(0, Math.min(index + 1, length)));
                        int index2 = country.indexOf('市', index);
                        if (index2 > 0) {
                            info.setCity(
                                    country.substring(index + 1, Math.min(index2 + 1, length)));
                        }
                    } else {
                        // 单独的处理
                        String flag = country.substring(0, 2);
                        switch (flag) {
                        case "内蒙":
                            info.setCountry("中国");
                            info.setProvince("内蒙古自治区");
                            country = country.substring(3);
                            if (country != null && !country.isEmpty()) {
                                index = country.indexOf('市');
                                if (index > 0) {
                                    info.setCity(country.substring(0,
                                            Math.min(index + 1, country.length())));
                                }
                                // :TODO 针对是旗、盟之类的不考虑
                            }
                            break;
                        case "广西":
                        case "宁夏":
                        case "西藏":
                        case "新疆":
                            info.setCountry("中国");
                            info.setProvince(flag);
                            country = country.substring(2);
                            if (country != null && !country.isEmpty()) {
                                index = country.indexOf('市');
                                if (index > 0) {
                                    info.setCity(country.substring(0,
                                            Math.min(index + 1, country.length())));
                                }
                            }
                            break;
                        case "上海":
                        case "北京":
                        case "重庆":
                        case "天津":
                            info.setCountry("中国");
                            info.setProvince(flag + "市");
                            country = country.substring(3);
                            if (country != null && !country.isEmpty()) {
                                index = country.indexOf('区');
                                if (index > 0) {
                                    char ch = country.charAt(index - 1);
                                    if (ch != '小' && ch != '校') {
                                        info.setCity(country.substring(0,
                                                Math.min(index + 1, country.length())));
                                    }
                                }
                            }

                            if (GlobalConstants.DEFAULT_VALUE.equals(info.getCity())) {
                                // 没有区，可能是县
                                index = country.indexOf('县');
                                if (index > 0) {
                                    info.setCity(country.substring(0,
                                            Math.min(index + 1, country.length())));
                                }
                            }
                            break;
                        case "香港":
                        case "澳门":
                            info.setCountry("中国");
                            info.setProvince(flag + "特别行政区");
                            break;
                        default:
                            info.setCountry(country); // 针对其他国家
                            break;
                        }
                    }
                }
            }
        }
        return info;
    }

    /**
     * 地域描述信息内部类
     * 
     * @author ibf
     *
     */
    public static class RegionInfo {
        private String country = GlobalConstants.DEFAULT_VALUE;
        private String province = GlobalConstants.DEFAULT_VALUE;
        private String city = GlobalConstants.DEFAULT_VALUE;

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getProvince() {
            return province;
        }

        public void setProvince(String province) {
            this.province = province;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        @Override
        public String toString() {
            return "RegionInfo [country=" + country + ", province=" + province + ", city=" + city
                    + "]";
        }
    }
}
