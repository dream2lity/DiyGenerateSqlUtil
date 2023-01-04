package org.example.sql.function.impl.functions.impl;

import org.example.enums.FieldTypeEnum;
import org.example.sql.function.impl.functions.AbstractFunctionHandler;

import static org.example.sql.function.impl.ColumnELConvertListener.TreeNodeInfo;

/**
 * @author chijiuwang
 */
public class ModFunctionHandler extends AbstractFunctionHandler {

    private static final int ARG_SIZE = 2;

    @Override
    public TreeNodeInfo processed() {
        if (this.expressionContexts.size() != ARG_SIZE) {
            throw new IllegalArgumentException("MOD(数值,数值):不符合的参数要求");
        }
        TreeNodeInfo dividend = getParseTreeNode(this.expressionContexts.get(0)),
                divisor = getParseTreeNode(this.expressionContexts.get(1));
        if (dividend.getType() != FieldTypeEnum.NUMBER || divisor.getType() != FieldTypeEnum.NUMBER) {
            throw new IllegalArgumentException("MOD(数值,数值):不符合的参数要求");
        }
        String expr = String.format("CASE WHEN %s = 0 THEN ((null + 0E0))\n" +
                        "ELSE MOD(%s, %s)\n" +
                        "END",
                divisor.getRealExpr(),
                dividend.getRealExpr(), divisor.getRealExpr());
        return generateTreeNode(expr, FieldTypeEnum.NUMBER);
    }
}
