package org.example.sql.function.impl.functions.impl;

import org.example.enums.FieldTypeEnum;
import org.example.sql.function.impl.functions.AbstractFunctionHandler;
import org.example.sql.function.impl.ColumnELConvertListener;

/**
 * @author chijiuwang
 */
public class PowerFunctionHandler extends AbstractFunctionHandler {

    private static final int ARG_SIZE = 2;

    @Override
    public ColumnELConvertListener.TreeNodeInfo processed() {
        if (this.expressionContexts.size() != ARG_SIZE) {
            throw new IllegalArgumentException("POWER(数值,数值):不符合的参数要求");
        }
        ColumnELConvertListener.TreeNodeInfo base = getParseTreeNode(this.expressionContexts.get(0)),
                power = getParseTreeNode(this.expressionContexts.get(1));
        if (base.getType() != FieldTypeEnum.NUMBER || power.getType() != FieldTypeEnum.NUMBER) {
            throw new IllegalArgumentException("POWER(数值,数值):不符合的参数要求");
        }
        String expr = String.format("POWER(%s, %s)", base.getRealExpr(), power.getRealExpr());
        return generateTreeNode(expr, FieldTypeEnum.NUMBER);
    }
}
