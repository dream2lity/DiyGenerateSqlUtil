package org.example.sql.operator;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.example.enums.AnalysisOperatorEnum;
import org.example.enums.TableTypeEnum;
import org.example.sql.base.Field;
import org.example.sql.base.WithAnyProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * 选字段 操作
 * @author chijiuwang
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class SelectFieldOperator extends AnalysisOperator<SelectFieldOperator.Fields> {

    public SelectFieldOperator() {
        this.type = AnalysisOperatorEnum.select_field;
        this.customName = AnalysisOperatorEnum.select_field.getDesc();
    }

    @EqualsAndHashCode(callSuper = false)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Data
    public static class Fields extends WithAnyProperties {
        /**
         * @see TableTypeEnum
         */
        private Integer tableType;
        private Long tableId;
        /**
         * 表名称 <br/>
         * <li>DB数据集时，这里是真实的物理表名称</li>
         * <li>自助数据集时，这里是数据集名称</li>
         */
        private String tableName;
        private List<Field> fields;
    }

}
