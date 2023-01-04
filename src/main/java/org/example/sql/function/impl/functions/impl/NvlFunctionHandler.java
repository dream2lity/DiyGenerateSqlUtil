package org.example.sql.function.impl.functions.impl;

import org.example.enums.FieldTypeEnum;
import org.example.sql.function.impl.functions.AbstractFunctionHandler;
import org.example.sql.function.ColumnELParser;
import org.example.sql.function.impl.ColumnELConvertListener;

import java.util.StringJoiner;

/**
 * @author chijiuwang
 */
public class NvlFunctionHandler extends AbstractFunctionHandler {
    private static final int MIN_ARG_SIZE = 1;

    @Override
    public ColumnELConvertListener.TreeNodeInfo processed() {
        if (this.expressionContexts.size() < MIN_ARG_SIZE) {
            throw new IllegalArgumentException("NVL(任意类型...):不符合的参数要求");
        }
        FieldTypeEnum finalType = getParseTreeNode(this.expressionContexts.get(0)).getType();
        ColumnELConvertListener.TreeNodeInfo arg;
        StringJoiner joiner = new StringJoiner(",\n\t", "COALESCE(\n\t", "\n\t)");
        for (ColumnELParser.ExpressionContext expressionContext : this.expressionContexts) {
            arg = getParseTreeNode(expressionContext);
            if (arg.getType() != finalType) {
                throw new IllegalArgumentException("NVL:不符合所有参数为相同类型的要求");
            }
            joiner.add(arg.getRealExpr());
        }
        return generateTreeNode(joiner.toString(), finalType);
    }
}
