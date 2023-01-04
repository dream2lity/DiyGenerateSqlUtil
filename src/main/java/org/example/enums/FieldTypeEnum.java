package org.example.enums;

/**
 * @author chijiuwang
 */
public enum FieldTypeEnum {
    /**
     * 字段类型
     */
    STRING(0, "文本", "string"),
    NUMBER(1, "数值", "number"),
    DATE(2, "日期", "date"),
    DATETIME(3, "时间", "datetime")
    ;

    int code;
    String desc;
    String innerDesc;

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    public String getInnerDesc() {
        return innerDesc;
    }

    FieldTypeEnum(int code, String desc, String innerDesc) {
        this.code = code;
        this.desc = desc;
        this.innerDesc = innerDesc;
    }

    public static FieldTypeEnum convertByCode(int code) {
        for (FieldTypeEnum t : FieldTypeEnum.values()) {
            if (t.getCode() == code) {
                return t;
            }
        }
        throw new IllegalArgumentException("FieldType unknown.");
    }
    public static String getInnerDescByCode(int code) {
        for (FieldTypeEnum t : FieldTypeEnum.values()) {
            if (t.getCode() == code) {
                return t.innerDesc;
            }
        }
        throw new IllegalArgumentException("FieldType unknown.");
    }

    public static FieldTypeEnum convertByInnerDesc(String str) {
        for (FieldTypeEnum t : FieldTypeEnum.values()) {
            if (t.getInnerDesc().equals(str) ) {
                return t;
            }
        }
        throw new IllegalArgumentException("FieldType unknown.");
    }

    //字段类型：0：数值 1：字符串 2：日期
//    DECIMAL(0,"DECIMAL", "数值"),STRING(1,"STRING", "字符串"), TIMESTAMP(2,"TIMESTAMP", "日期");


//
//    /**
//     * mysql字段类型转换成数据集字段类型
//     * @param mysqlColumnTypeStr
//     * @return
//     */
//    public static FieldTypeEnum convertFromMysql(String mysqlColumnTypeStr) {
//        MysqlColumnType mysqlColumnType = MysqlColumnType.getColumnType(mysqlColumnTypeStr);
//
//        switch (mysqlColumnType) {
//            case BIT:
//            case TINYINT:
//            case SMALLINT:
//            case MEDIUMINT:
//            case INTEGER:
//            case INT:
//            case BIGINT:
//            case FLOAT:
//            case DOUBLE:
//            case DECIMAL:
//                return FieldTypeEnum.NUMBER;
//            case CHAR:
//            case VARCHAR:
//            case TEXT:
//            case LONGTEXT:
//            case JSON:
//                return FieldTypeEnum.STRING;
//            case DATE:
//            case YEAR:
//            case DATETIME:
//            case TIMESTAMP:
//                return FieldTypeEnum.DATE;
//            default:
//                return FieldTypeEnum.STRING;
//        }
//    }
//
//
//    /**
//     * 根据schema数据类型获取数据集列类型
//     * @param s
//     * @return
//     */
//    public static int getDatasetColumnTypeBySchemaType(Schema s) {
//        String mysqlTypeStr;
//        //设置字段长度
//        if(s.getDataType().matches("[a-zA-Z]+\\([1-9]+[0-9]*(,[1-9]+[0-9]*){0,1}\\)( unsigned){0,1}( zerofill){0,1} *")){
//            String dataType = s.getDataType().replaceAll(" unsigned","").replaceAll("( zerofill){0,1} *","");
//            mysqlTypeStr=dataType.substring(0,dataType.indexOf("("));
//        }else{
//            mysqlTypeStr= s.getDataType();
//        }
//        return FieldTypeEnum.convertFromMysql(mysqlTypeStr).ordinal();
//    }
}
