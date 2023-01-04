package org.example.enums;

/**
 * @author chijiuwang
 */
public enum FieldTypeTransformEnum {
    /**
     * 字段类型转换 <br/>
     * 扩展方式：<br/>
     * 增加这个枚举的实例，所有属性都赋值即可<br/>
     * 这里的functionFormat将用于String.format中的第一个参数，这里%需要转义为%%， %s标识字段的位置
     *
     */
    NUMBER_TO_TEXT(1, 0, "cast(%s as char)", 1),
    DATE_TO_TEXT(2, 0, "date_format(%s,'%%Y-%%m-%%d')", 1),
    DATETIME_TO_TEXT(3, 0, "date_format(%s,'%%Y-%%m-%%d %%H:%%i:%%s')", 1),

    TEXT_TO_NUMBER(0, 1, "((cast(%s as char) + 0E0))", 1),
    DATE_TO_NUMBER(2, 1, "(unix_timestamp(%s) * 1000)", 1),
    DATETIME_TO_NUMBER(3, 1, "(unix_timestamp(%s) * 1000)", 1),

    TEXT_TO_DATE(0, 2, "date(%s)", 1),
    NUMBER_TO_DATE(1, 2, "date(from_unixtime((%s / 1000)))", 1),
    DATETIME_TO_DATE(3, 2, "date(%s)", 1),

    TEXT_TO_DATETIME(0, 3, "timestamp(%s)", 1),
    NUMBER_TO_DATETIME(1, 3, "from_unixtime((%s / 1000))", 1),
    DATE_TO_DATETIME(2, 3, "timestamp(%s)", 1),
    ;

    int fromType;
    int toType;
    String functionFormat;
    int argCount;

    FieldTypeTransformEnum(int fromType, int toType, String functionFormat, int argCount) {
        this.fromType = fromType;
        this.toType = toType;
        this.functionFormat = functionFormat;
        this.argCount = argCount;
    }

    public int getFromType() {
        return fromType;
    }

    public int getToType() {
        return toType;
    }

    public String getFunctionFormat() {
        return functionFormat;
    }

    public int getArgCount() {
        return argCount;
    }

    public static FieldTypeTransformEnum convertByToType(int fromType, int toType) {
        for (FieldTypeTransformEnum t : FieldTypeTransformEnum.values()) {
            if (t.getFromType() == fromType && t.getToType() == toType) {
                return t;
            }
        }
        throw new IllegalArgumentException("FieldTypeTransform unknown.");
    }
}
