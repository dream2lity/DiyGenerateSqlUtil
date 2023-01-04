package org.example.sql.operator;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.enums.AnalysisOperatorEnum;
import org.example.enums.FilterTypeEnum;
import org.example.sql.base.OperatorField;
import org.example.sql.base.WithAnyProperties;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.apache.commons.lang3.ArrayUtils;

import java.util.List;

/**
 * 过滤 操作
 * @author chijiuwang
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class FilterOperator extends AnalysisOperator<FilterOperator.Filters<Object>> {

    public FilterOperator() {
        this.type = AnalysisOperatorEnum.filter;
        this.customName = AnalysisOperatorEnum.filter.getDesc();
    }

    /**
     * 转换filterValue为具体的Filters
     * @param filter 当前值
     * @param conditionFilterTypes 需要转换的filterType
     * @param objectMapper 转换器
     */
    public void transferFilterValue(Filters<Object> filter, Integer[] conditionFilterTypes, ObjectMapper objectMapper) {
        if (ArrayUtils.contains(conditionFilterTypes, filter.filterType)) {
            List<Filters<Object>> filters = objectMapper.convertValue(filter.children, new TypeReference<List<Filters<Object>>>() {
            });
            filter.filterValue = filters;
            for (Filters<Object> f : filters) {
                transferFilterValue(f, conditionFilterTypes, objectMapper);
            }
        }
    }

    @EqualsAndHashCode(callSuper = false)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @ToString
    @Data
    public static class Filters<VT> extends WithAnyProperties {
        /**
         * {@link OperatorField} 的fieldId
         */
        private String fieldId;
        /**
         * @see FilterTypeEnum
         */
        private Integer filterType;
        /**
         * 具体参数格式根据filterType决定
         */
        private VT filterValue;
        /**
         * 当前且和或的时候用
         */
        private List<Filters<?>> children;
    }

}
