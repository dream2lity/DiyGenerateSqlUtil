package org.example.sql.function.impl.functions.impl;

import org.example.enums.FieldTypeEnum;
import org.example.sql.function.impl.functions.AbstractFunctionHandler;

import java.util.StringJoiner;

import static org.example.sql.function.impl.ColumnELConvertListener.TreeNodeInfo;

/**
 * @author chijiuwang
 */
public class SwitchFunctionHandler extends AbstractFunctionHandler {

    private static final int MIN_ARG_SIZE = 3;
    private static final int ARG_PARI_SIZE = 2;

    @Override
    public TreeNodeInfo processed() {
        int argSize = this.expressionContexts.size();
        if (argSize < MIN_ARG_SIZE || argSize % ARG_PARI_SIZE != 1) {
            throw new IllegalArgumentException("SWITCH:参数个数不对");
        }

        TreeNodeInfo expr = getParseTreeNode(this.expressionContexts.get(0)), exprResult, result;
        StringJoiner joiner = new StringJoiner("", String.format("CASE %s ", expr.getExpr()), " \n\tEND");
        FieldTypeEnum resultType = getParseTreeNode(this.expressionContexts.get(MIN_ARG_SIZE - 1)).getType();
        for (int i = 1; i < argSize; i = i + ARG_PARI_SIZE) {
            exprResult = getParseTreeNode(this.expressionContexts.get(i));
            result = getParseTreeNode(this.expressionContexts.get(i + 1));
            if (resultType != result.getType()) {
                throw new IllegalArgumentException("SWITCH:表达式的结果类型必须相同");
            }
            joiner.add(String.format("\n\tWHEN %s THEN %s", exprResult.getExpr(), result.getRealExpr()));
        }
        return generateTreeNode(joiner.toString(), resultType);
    }
}
