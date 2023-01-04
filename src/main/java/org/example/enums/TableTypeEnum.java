package org.example.enums;

/**
 * @author chijiuwang
 */
public enum TableTypeEnum {
    /**
     * 1：DB数据集 ,2:excel数据集,3:SQL数据集,4：自助数据集
     */
    DB(1, "DB数据集"),
    EXCEL(2, "excel数据集"),
    SQL(3, "SQL数据集"),
    DIY(4, "自助数据集"),
    ;
    int code;
    String desc;

    TableTypeEnum(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    public static TableTypeEnum convertByCode(int code) {
        for (TableTypeEnum t : TableTypeEnum.values()) {
            if (t.getCode() == code) {
                return t;
            }
        }
        throw new IllegalArgumentException("TableType unknown.");
    }
}
