package org.example.sql.operator;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.example.enums.AnalysisOperatorEnum;
import lombok.Data;
import org.example.sql.OperatorSqlAdapter;

import java.util.Map;

/**
 * <h3>操作基类</h3>
 * 作为operators的参数模型，在{@link OperatorSqlAdapter}中被解析为SQL <br/>
 * <br/>
 * 扩展操作方式：<br/>
 * <ol>
 *  <li>继承这个类，定义自己的value格式</li>
 *  <li>在{@link AnalysisOperatorEnum}中注册这个类，注册枚举中相应的属性</li>
 * </ol>
 *
 * <br/>
 * @author chijiuwang
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type", visible = true)
@Data
public class AnalysisOperator<T> {
    /**
     * 操作类型，根据这个值确定具体是哪个子类
     */
    protected AnalysisOperatorEnum type;
    /**
     * 操作名称
     */
    protected String customName;
    /**
     * 暂时未用
     */
    protected String comment;
    /**
     * 暂时未用
     */
    protected Map<String, String> sourceToResultFieldNameMap;
    /**
     * 操作中的具体参数，根据type的值确定
     */
    protected T value;

}
