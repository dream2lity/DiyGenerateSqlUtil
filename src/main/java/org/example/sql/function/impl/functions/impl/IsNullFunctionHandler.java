package org.example.sql.function.impl.functions.impl;

import org.example.enums.FieldTypeEnum;
import org.example.sql.function.impl.functions.AbstractFunctionHandler;
import org.example.sql.function.impl.ColumnELConvertListener;

/**
 * @author chijiuwang
 */
public class IsNullFunctionHandler extends AbstractFunctionHandler {

    private static final int ARG_SIZE = 1;

    @Override
    public ColumnELConvertListener.TreeNodeInfo processed() {
        if (this.expressionContexts.size() != ARG_SIZE) {
            throw new IllegalArgumentException("ISNULL(任意类型):不符合的参数要求");
        }
        String expr = String.format("cast((%s is null) as signed)", getParseTreeNode(this.expressionContexts.get(0)).getExpr());
        return generateTreeNode(expr, FieldTypeEnum.NUMBER);
    }
}
