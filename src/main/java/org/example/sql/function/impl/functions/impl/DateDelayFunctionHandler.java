package org.example.sql.function.impl.functions.impl;

import org.example.enums.FieldTypeEnum;
import org.example.sql.function.impl.functions.AbstractFunctionHandler;
import org.example.sql.function.impl.ColumnELConvertListener;

/**
 * @author chijiuwang
 */
public class DateDelayFunctionHandler extends AbstractFunctionHandler {

    private static final int ARG_SIZE = 2;

    @Override
    public ColumnELConvertListener.TreeNodeInfo processed() {
        if (this.expressionContexts.size() != ARG_SIZE) {
            throw new IllegalArgumentException("DATEDELAY(日期/文本,数值):不符合的参数要求");
        }
        ColumnELConvertListener.TreeNodeInfo date = getParseTreeNode(this.expressionContexts.get(0)),
                interval = getParseTreeNode(this.expressionContexts.get(1));
        if (isDateOrStringType(date.getType()) && isNumberType(interval.getType())) {
            String expr = String.format("DATE_ADD(%s, INTERVAL %s DAY)", getFinalDate(date), interval.getRealExpr());
            return generateTreeNode(expr, FieldTypeEnum.DATETIME);
        } else {
            throw new IllegalArgumentException("DATEDELAY(日期/文本,数值):不符合的参数要求");
        }
    }
}
