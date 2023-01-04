package org.example.sql.function.impl.functions.impl;

import org.example.enums.FieldTypeEnum;
import org.example.sql.function.impl.functions.AbstractFunctionHandler;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.example.sql.function.impl.ColumnELConvertListener.TreeNodeInfo;

/**
 *
 * yyyy-MM-dd HH:mm:ss.SSS aa hh
 * %Y-%m-%d %H:%i:%s.%f %p %h
 *
 * @author chijiuwang
 */
public class FormatFunctionHandler extends AbstractFunctionHandler {

    private static final int ARG_SIZE = 2;

    private static final Map<String, String> DATE_FORMAT_MAPS = new LinkedHashMap<String, String>(){{
        put("yyyy", "%Y");
        put("MM", "%m");
        put("dd", "%d");
        put("HH", "%H");
        put("hh", "%h");
        put("mm", "%i");
        put("ss", "%s");
        put("SSS", "%f");
        put("aa", "%p");
    }};

    @Override
    public TreeNodeInfo processed() {
        if (this.expressionContexts.size() != ARG_SIZE) {
            throw new IllegalArgumentException("FORMAT(日期,文本):不符合的参数要求");
        }
        TreeNodeInfo date = getParseTreeNode(this.expressionContexts.get(0)),
                format = getParseTreeNode(this.expressionContexts.get(1));
        if (isDateType(date.getType()) && isStringType(format.getType())) {
            String formatExpr = format.getExpr();
            for (Map.Entry<String, String> entry : DATE_FORMAT_MAPS.entrySet()) {
                formatExpr = formatExpr.replaceAll(entry.getKey(), entry.getValue());
            }
            return generateTreeNode(String.format("DATE_FORMAT(%s, %s)", date.getExpr(), formatExpr), FieldTypeEnum.STRING);
        } else {
            throw new IllegalArgumentException("FORMAT(日期,文本):不符合的参数要求");
        }
    }
}
