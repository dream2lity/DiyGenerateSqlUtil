package org.example.sql.function.impl.functions.impl;


import org.example.enums.FieldTypeEnum;
import org.example.sql.function.impl.functions.AbstractFunctionHandler;
import org.example.sql.function.impl.ColumnELConvertListener;

/**
 * @author chijiuwang
 */
public class RoundFunctionHandler extends AbstractFunctionHandler {

    private static final int ARG_SIZE = 2;

    @Override
    public ColumnELConvertListener.TreeNodeInfo processed() {
        if (this.expressionContexts.size() != ARG_SIZE) {
            throw new IllegalArgumentException("ROUND(数值,数值):不符合的参数要求");
        }
        ColumnELConvertListener.TreeNodeInfo number = getParseTreeNode(this.expressionContexts.get(0)),
                digits = getParseTreeNode(this.expressionContexts.get(1));
        if (isNumberType(number.getType()) && isNumberType(digits.getType())) {
            return generateTreeNode(
                    String.format("ROUND(\n" +
                    "    %s,\n" +
                    "    %s\n" +
                    ")", number.getRealExpr(), digits.getRealExpr()),
                    FieldTypeEnum.NUMBER);
        } else {
            throw new IllegalArgumentException("ROUND(数值,数值):不符合的参数要求");
        }
    }
}
