package org.example.enums;

/**
 * @author chijiuwang
 */
public enum DateFunctionEnum {
    /**
     * 日期类型函数，用于分组字段 <br/>
     * 扩展方式：<br/>
     * 增加这个枚举的实例，所有属性都赋值即可<br/>
     * 这里的functionFormat将用于String.format中的第一个参数，这里%需要转义为%%， %s标识字段的位置
     *
     */
    DATE(1, "年 月 日", "date(%s)", 1),
    MONTH(2, "年 月", "str_to_date(date_format(%s, '%%Y-%%m-01'), '%%Y-%%m-%%d')", 1),
    HOUR(3, "年 月 日 时", "str_to_date(date_format(%s, '%%Y-%%m-%%d %%H:00:00.0'), '%%Y-%%m-%%d %%H:%%i:%%s.0')", 1),
    WEEK(4, "周", "DATE_FORMAT(DATE_ADD(%s, INTERVAL -WEEKDAY(%s) DAY),'%%Y-%%m-%%d')", 2),
    ;

    int code;
    String desc;
    String functionFormat;
    int argCount;

    DateFunctionEnum(int code, String desc, String functionFormat, int argCount) {
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

    public static DateFunctionEnum convertByCode(int code) {
        for (DateFunctionEnum t : DateFunctionEnum.values()) {
            if (t.getCode() == code) {
                return t;
            }
        }
        throw new IllegalArgumentException("DateFunc unknown.");
    }
}
