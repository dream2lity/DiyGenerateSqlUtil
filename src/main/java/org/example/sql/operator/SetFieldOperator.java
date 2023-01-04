package org.example.sql.operator;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.example.enums.AnalysisOperatorEnum;
import org.example.enums.FieldTypeEnum;
import org.example.sql.base.OperatorField;
import org.example.sql.base.WithAnyProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * 字段设置 操作
 * @author chijiuwang
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class SetFieldOperator extends AnalysisOperator<SetFieldOperator.FieldList> {

    public SetFieldOperator() {
        this.type = AnalysisOperatorEnum.set_field;
        this.customName = AnalysisOperatorEnum.set_field.getDesc();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Data
    public static class FieldList {
        private List<FieldSetters> fieldList;
    }

    @EqualsAndHashCode(callSuper = false)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Data
    public static class FieldSetters extends WithAnyProperties {
        /**
         * {@link OperatorField} 的fieldId
         */
        private String fieldId;
        /**
         * 字段名称，最终展示的名称，可以是原始名称，也可以是重命名后的新名称
         */
        private String transferName;
        /**
         * @see FieldTypeEnum
         */
        private Integer fieldType;
        /**
         * 是否使用该字段
         */
        private Boolean used;
        /**
         * 该字段类型是否发生改变，前端不给传了，需要自行判断
         */
        private Boolean typeSpecified;
    }

}
