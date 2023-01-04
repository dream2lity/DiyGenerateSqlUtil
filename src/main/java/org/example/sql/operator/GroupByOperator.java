package org.example.sql.operator;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.example.enums.*;
import org.example.sql.base.WithAnyProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.example.sql.base.OperatorField;

import java.util.List;

/**
 * 分组汇总 操作
 * @author chijiuwang
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class GroupByOperator extends AnalysisOperator<GroupByOperator.GroupInfo> {

    public GroupByOperator() {
        this.type = AnalysisOperatorEnum.group_by;
        this.customName = AnalysisOperatorEnum.group_by.getDesc();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Data
    public static class GroupInfo {
        /**
         * 分组字段
         */
        private List<GroupByFieldInfo> dimensions;
        /**
         * 汇总字段
         */
        private List<GroupByFieldInfo> norms;
    }

    @EqualsAndHashCode(callSuper = false)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @NoArgsConstructor
    @Data
    public static class GroupByFieldInfo extends WithAnyProperties {
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
         * 分组字段可选值：<br/>
         * <li>{@link DateFunctionEnum}</li>
         *
         * 汇总字段可选值：<br/>
         * <li>{@link TextAggFuncEnum}</li>
         * <li>{@link NumberAggFuncEnum}</li>
         * <li>{@link DateAggFuncEnum}</li>
         */
        private Integer functionType;

        public GroupByFieldInfo(String fieldId, String transferName, Integer fieldType, Integer functionType) {
            this.fieldId = fieldId;
            this.transferName = transferName;
            this.fieldType = fieldType;
            this.functionType = functionType;
        }
    }

}
