package org.example.enums;

/**
 * @author chijiuwang
 */
public enum TextFunctionEnum {
    /**
     * 文本类型函数，用于分组字段 <br/>
     * 扩展方式：<br/>
     * 增加这个枚举的实例，所有属性都赋值即可<br/>
     * 这里的functionFormat将用于String.format中的第一个参数，这里%需要转义为%%， %s标识字段的位置
     *
     */
    SAME(1, "相同值为一组", "", 0)
    ;

    int code;
    String desc;
    String functionFormat;
    int argCount;

    TextFunctionEnum(int code, String desc, String functionFormat, int argCount) {
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

    public static TextFunctionEnum convertByCode(int code) {
        for (TextFunctionEnum t : TextFunctionEnum.values()) {
            if (t.getCode() == code) {
                return t;
            }
        }
        throw new IllegalArgumentException("DateFunc unknown.");
    }
}
