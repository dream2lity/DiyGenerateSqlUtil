package org.example.sql.function.impl.functions.impl;

import org.example.enums.FieldTypeEnum;
import org.example.sql.function.impl.functions.AbstractFunctionHandler;
import org.example.sql.function.impl.ColumnELConvertListener;

/**
 * @author chijiuwang
 */
public class MidFunctionHandler extends AbstractFunctionHandler {

    private static final int ARG_SIZE = 3;

    @Override
    public ColumnELConvertListener.TreeNodeInfo processed() {
        if (this.expressionContexts.size() != ARG_SIZE) {
            throw new IllegalArgumentException("MID(文本,数值,数值):不符合的参数要求");
        }
        ColumnELConvertListener.TreeNodeInfo text = getParseTreeNode(this.expressionContexts.get(0)),
                start = getParseTreeNode(this.expressionContexts.get(1)),
                len = getParseTreeNode(this.expressionContexts.get(2));
        if (isStringType(text.getType()) && isNumberType(start.getType()) && isNumberType(len.getType())) {

            return generateTreeNode(
                    String.format("CASE WHEN (\n" +
                            "      %s > 0\n" +
                            "      AND %s > 0\n" +
                            "    ) THEN SUBSTRING(\n" +
                            "      %s,\n" +
                            "      %s,\n" +
                            "      %s\n" +
                            "    )\n" +
                            "     ELSE NULL\n" +
                            "END", start.getRealExpr(), len.getRealExpr(), text.getExpr(), start.getRealExpr(), len.getRealExpr()),
                    FieldTypeEnum.STRING
            );
        } else {
            throw new IllegalArgumentException("MID(文本,数值,数值):不符合的参数要求");
        }
    }
}
