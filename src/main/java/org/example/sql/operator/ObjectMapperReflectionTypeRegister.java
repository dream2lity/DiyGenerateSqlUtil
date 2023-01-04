package org.example.sql.operator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import org.example.enums.AnalysisOperatorEnum;
import org.reflections.Reflections;

import java.util.Set;

/**
 * @author chijiuwang
 */
public class ObjectMapperReflectionTypeRegister {

    /**
     * 扫描 {@link AnalysisOperator} 的子类， 加载到共用的 ObjectMapper 中 <br/>
     * 实现自动解析controller参数中的{@link AnalysisOperator}为对应的子类
     */

    public ObjectMapperReflectionTypeRegister(ObjectMapper objectMapper) {
        Reflections reflections = new Reflections(AnalysisOperator.class.getPackage().getName());
        @SuppressWarnings("rawtypes")
        Set<Class<? extends AnalysisOperator>> types = reflections.getSubTypesOf(AnalysisOperator.class);
        types.forEach(t -> {
            AnalysisOperatorEnum analysisOperatorEnum = AnalysisOperatorEnum.convertByClassType(t);
            objectMapper.registerSubtypes(new NamedType(t, analysisOperatorEnum.name()));
        });

    }

}

