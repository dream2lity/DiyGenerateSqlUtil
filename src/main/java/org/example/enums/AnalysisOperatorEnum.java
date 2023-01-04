package org.example.enums;

import org.example.sql.operator.*;

/**
 * @author chijiuwang
 */
public enum AnalysisOperatorEnum {
    /**
     * 操作类型：选字段、左右合并、过滤、字段设置、分组汇总、新增列、上下合并 <br/>
     * 扩展操作需要指定对应的classType <br/>
     * 所支持的操作
     */
    select_field(1, "选字段", SelectFieldOperator.class),
    join(2, "左右合并", JoinOperator.class),
    filter(3, "过滤", FilterOperator.class),
    set_field(4, "字段设置", SetFieldOperator.class),
    group_by(5, "分组汇总", GroupByOperator.class),
    add_field(6, "新增列", AddFieldOperator.class),
//    union(7, "上下合并"),
    ;

    final int code;
    final String desc;
    final Class<?> classType;

    AnalysisOperatorEnum(int code, String desc, Class<?> classType) {
        this.code = code;
        this.desc = desc;
        this.classType = classType;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    public static AnalysisOperatorEnum convertByClassType(Class<?> classType) {
        for (AnalysisOperatorEnum operatorEnum : AnalysisOperatorEnum.values()) {
            if (operatorEnum.classType.equals(classType)) {
                return operatorEnum;
            }
        }
        throw new IllegalArgumentException("unknown operator type.");
    }
}
