package org.example.sql.operator;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.example.enums.AnalysisOperatorEnum;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.example.enums.JoinTypeEnum;
import org.example.sql.base.OperatorField;

import java.util.List;

/**
 * 左右合并 操作
 * @author chijiuwang
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class JoinOperator extends AnalysisOperator<JoinOperator.JoinInfo> {

    public JoinOperator() {
        this.type = AnalysisOperatorEnum.join;
        this.customName = AnalysisOperatorEnum.join.getDesc();
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Data
    public static class JoinInfo {
        /**
         * @see JoinTypeEnum
         */
        private Integer style;
        /**
         * 关联关系格式 <br/>
         * [[当前表关联字段, 新表关联字段], [..] ..] <br/>
         * <li>当前表关联字段指当前表字段的fieldId，{@link OperatorField}</li>
         * <li>新表关联字段指table中字段的fieldName</li>
         */
        private List<List<String>> basis;
        private SelectFieldOperator.Fields table;
    }

}
