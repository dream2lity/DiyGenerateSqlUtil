package org.example.sql.function.impl.functions.impl;

import org.example.enums.FieldTypeEnum;
import org.example.sql.function.impl.functions.AbstractFunctionHandler;
import org.example.sql.function.ColumnELParser;
import org.example.sql.function.impl.ColumnELConvertListener;

import java.util.StringJoiner;

/**
 * @author chijiuwang
 */
public class MaxFunctionHandler extends AbstractFunctionHandler {

    private static final int MIN_ARG_SIZE = 1;

    protected String functionName = "MAX";
    protected String functionRealName = "GREATEST";

    @Override
    public ColumnELConvertListener.TreeNodeInfo processed() {
        if (this.expressionContexts.size() < MIN_ARG_SIZE) {
            throw new IllegalArgumentException(functionName + "(数值,...):不符合的参数要求");
        }
        ColumnELConvertListener.TreeNodeInfo expr;
        StringJoiner joiner = new StringJoiner(", \n\t", functionRealName + "( \n\t", " \n\t)");
        for (ColumnELParser.ExpressionContext expressionContext : this.expressionContexts) {
            expr = getParseTreeNode(expressionContext);
            if (expr.getType() != FieldTypeEnum.NUMBER) {
                throw new IllegalArgumentException(functionName + "(数值,...):不符合的参数要求");
            }
            joiner.add(expr.getRealExpr());
        }
        return generateTreeNode(joiner.toString(), FieldTypeEnum.NUMBER);
    }
}
