package org.example.sql.function.impl.functions.impl;

import org.example.enums.FieldTypeEnum;
import org.example.sql.function.impl.functions.AbstractFunctionHandler;
import org.example.sql.function.impl.ColumnELConvertListener;

/**
 * @author chijiuwang
 */
public class RandBetweenFunctionHandler extends AbstractFunctionHandler {

    private static final int ARG_SIZE = 2;

    @Override
    public ColumnELConvertListener.TreeNodeInfo processed() {
        if (this.expressionContexts.size() != ARG_SIZE) {
            throw new IllegalArgumentException("RANDBETWEEN(数值,数值):不符合的参数要求");
        }
        ColumnELConvertListener.TreeNodeInfo min = getParseTreeNode(this.expressionContexts.get(0)),
                max = getParseTreeNode(this.expressionContexts.get(1));
        if (isNumberType(min.getType()) && isNumberType(max.getType())) {
            return generateTreeNode(
                    String.format("CEIL((FLOOR(%s) + (RAND() * (FLOOR(%s) - FLOOR(%s)))))", min.getRealExpr(), max.getRealExpr(), min.getRealExpr()),
                    FieldTypeEnum.NUMBER
            );
        } else {
            throw new IllegalArgumentException("RANDBETWEEN(数值,数值):不符合的参数要求");
        }
    }
}
