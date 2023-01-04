package org.example.sql.function.impl.functions.impl;

import org.example.enums.FieldTypeEnum;
import org.example.sql.function.impl.functions.AbstractFunctionHandler;
import org.example.sql.function.impl.ColumnELConvertListener;

/**
 * @author chijiuwang
 */
public class TimeFunctionHandler extends AbstractFunctionHandler {

    private static final int ARG_SIZE = 3;

    @Override
    public ColumnELConvertListener.TreeNodeInfo processed() {
        if (this.expressionContexts.size() != ARG_SIZE) {
            throw new IllegalArgumentException("TIME(数值,数值,数值):不符合的参数要求");
        }
        ColumnELConvertListener.TreeNodeInfo hour = getParseTreeNode(this.expressionContexts.get(0)),
                minute = getParseTreeNode(this.expressionContexts.get(1)),
                second = getParseTreeNode(this.expressionContexts.get(2));
        if (isNumberType(hour.getType()) && isNumberType(minute.getType()) && isNumberType(second.getType())) {
            return generateTreeNode(
                    String.format("STR_TO_DATE(\n" +
                            "  CONCAT(\n" +
                            "    DATE_FORMAT(CURRENT_DATE(),'%%Y-%%m-%%d'),\n" +
                            "    ' ',\n" +
                            "    CAST(%s AS CHAR),\n" +
                            "    ':',\n" +
                            "    CAST(%s AS CHAR),\n" +
                            "    ':',\n" +
                            "    CAST(%s AS CHAR)\n" +
                            "  ),\n" +
                            "  '%%Y-%%m-%%d %%H:%%i:%%s'\n" +
                            ")", hour.getRealExpr(), minute.getRealExpr(), second.getRealExpr()),
                    FieldTypeEnum.DATETIME
            );
        } else {
            throw new IllegalArgumentException("TIME(数值,数值,数值):不符合的参数要求");
        }
    }
}
