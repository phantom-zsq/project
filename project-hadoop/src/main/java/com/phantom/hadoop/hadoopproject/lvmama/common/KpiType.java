package com.phantom.hadoop.hadoopproject.lvmama.common;

/**
 * 统计kpi的名称枚举类
 * 
 * @author ibeifeng
 *
 */
public enum KpiType {
    NEW_INSTALL_USER("new_install_user"), // 统计新用户的kpi
    BROWSER_NEW_INSTALL_USER("browser_new_install_user"), // 统计浏览器维度的新用户kpi
    EVENT_TIMES("event_times") // 计算事件触发次数
    ;

    public final String name;

    private KpiType(String name) {
        this.name = name;
    }

    /**
     * 根据kpiType的名称字符串值，获取对应的kpitype枚举对象
     * 
     * @param name
     * @return
     */
    public static KpiType valueOfName(String name) {
        for (KpiType type : values()) {
            if (type.name.equals(name)) {
                return type;
            }
        }
        throw new RuntimeException("指定的name不属于该KpiType枚举类：" + name);
    }
}
