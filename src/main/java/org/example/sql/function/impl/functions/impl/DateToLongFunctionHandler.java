package org.example.sql.function.impl.functions.impl;

import org.example.enums.FieldTypeEnum;
import org.example.sql.function.impl.functions.AbstractFunctionHandler;

import static org.example.sql.function.impl.ColumnELConvertListener.TreeNodeInfo;

/**
 * @author chijiuwang
 */
public class DateToLongFunctionHandler extends AbstractFunctionHandler {

    private static final int ARG_SIZE = 1;

    @Override
    public TreeNodeInfo processed() {
        if (this.expressionContexts.size() != ARG_SIZE) {
            throw new IllegalArgumentException("DATETOLONG:(日期/文本) :不符合的参数要求");
        }
        TreeNodeInfo dateOrStr = getParseTreeNode(this.expressionContexts.get(0));
        if (isDateOrStringType(dateOrStr.getType())) {
            return generateTreeNode(
                    isStringType(dateOrStr.getType())
                    ? String.format("(UNIX_TIMESTAMP(DATE(%s)) * 1000)", dateOrStr.getExpr())
                    : String.format("(UNIX_TIMESTAMP(%s) * 1000)", dateOrStr.getExpr()),
                    FieldTypeEnum.NUMBER
            );
        } else {
            throw new IllegalArgumentException("DATETOLONG:(日期/文本) :不符合的参数要求");
        }
    }
}
