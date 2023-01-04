package org.example.sql.function.impl.functions.impl;

import org.example.enums.FieldTypeEnum;
import org.example.sql.function.impl.functions.AbstractFunctionHandler;

import static org.example.sql.function.impl.ColumnELConvertListener.TreeNodeInfo;

/**
 * @author chijiuwang
 */
public class ReplaceFunctionHandler extends AbstractFunctionHandler {

    private static final int ARG_SIZE = 3;

    @Override
    public TreeNodeInfo processed() {
        if (this.expressionContexts.size() != ARG_SIZE) {
            throw new IllegalArgumentException("REPLACE:不符合的参数要求");
        }
        return generateTreeNode(
                String.format("REPLACE(\n" +
                        "    %s,\n" +
                        "    %s,\n" +
                        "    %s\n" +
                        ")",
                        convertToStringExpression(getParseTreeNode(this.expressionContexts.get(0))),
                        convertToStringExpression(getParseTreeNode(this.expressionContexts.get(1))),
                        convertToStringExpression(getParseTreeNode(this.expressionContexts.get(2)))
                ),
                FieldTypeEnum.STRING
        );
    }
}
