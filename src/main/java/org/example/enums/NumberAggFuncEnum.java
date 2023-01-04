package org.example.enums;

/**
 * @author chijiuwang
 */
public enum NumberAggFuncEnum {
    /**
     * 数值类型 聚合函数 <br/>
     * 扩展方式：<br/>
     * 增加这个枚举的实例，所有属性都赋值即可<br/>
     * 这里的functionFormat将用于String.format中的第一个参数，这里%需要转义为%%， %s标识字段的位置
     *
     */
    SUM(1, "求和", "sum(%s)", 1),
    AVG(2, "平均", "avg(%s)", 1),
    MAX(3, "最大", "max(%s)", 1),
    MIN(4, "最小", "min(%s)", 1),
    COUNT(5, "记录个数", "count(%s)", 1),
    COUNT_DISTINCT(6, "去重计数", "count( distinct %s)", 1)
    ;

    int code;
    String desc;
    String functionFormat;
    int argCount;

    NumberAggFuncEnum(int code, String desc, String functionFormat, int argCount) {
        this.code = code;
        this.desc = desc;
        this.functionFormat = functionFormat;
        this.argCount = argCount;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    public String getFunctionFormat() {
        return functionFormat;
    }

    public int getArgCount() {
        return argCount;
    }

    public static NumberAggFuncEnum convertByCode(int code) {
        for (NumberAggFuncEnum t : NumberAggFuncEnum.values()) {
            if (t.getCode() == code) {
                return t;
            }
        }
        throw new IllegalArgumentException("NumberAggFunc unknown.");
    }
}
