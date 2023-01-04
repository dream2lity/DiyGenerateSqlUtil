package org.example.sql.function.impl.functions.impl;


import org.example.enums.FieldTypeEnum;
import org.example.sql.function.impl.functions.AbstractFunctionHandler;
import org.example.sql.function.impl.ColumnELConvertListener;

/**
 * @author chijiuwang
 */
public class UpperFunctionHandler extends AbstractFunctionHandler {

    private static final int ARG_SIZE = 1;

    protected String functionRealName = "UPPER";

    @Override
    public ColumnELConvertListener.TreeNodeInfo processed() {
        if (this.expressionContexts.size() != ARG_SIZE) {
            throw new IllegalArgumentException(functionRealName + "(文本):不符合的参数要求");
        }
        ColumnELConvertListener.TreeNodeInfo text = getParseTreeNode(this.expressionContexts.get(0));
        if (isStringType(text.getType())) {
            return generateTreeNode(String.format(functionRealName + "(%s)", text.getExpr()), FieldTypeEnum.STRING);
        } else {
            throw new IllegalArgumentException(functionRealName + "(文本):不符合的参数要求");
        }
    }
}
