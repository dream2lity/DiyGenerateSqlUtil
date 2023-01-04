package org.example.enums;

/**
 * @author chijiuwang
 */
public enum JoinTypeEnum {
    /**
     * 注：MySQL不支持full join，可以转换为[(left join) union all (right join)]
     */
    INNER_JOIN(1, "交集合并"),
    LEFT_OUTER_JOIN(2, "左合并"),
    RIGHT_OUTER_JOIN(3, "右合并"),
    FULL_OUTER_JOIN(4, "并集合并")
    ;

    int code;
    String desc;

    JoinTypeEnum(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    public static JoinTypeEnum convertByCode(int code) {
        for (JoinTypeEnum t : JoinTypeEnum.values()) {
            if (t.getCode() == code) {
                return t;
            }
        }
        throw new IllegalArgumentException("JoinType unknown.");
    }
}
