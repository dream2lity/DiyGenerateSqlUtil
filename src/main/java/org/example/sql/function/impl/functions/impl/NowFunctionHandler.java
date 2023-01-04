package org.example.sql.function.impl.functions.impl;

import org.example.enums.FieldTypeEnum;
import org.example.sql.function.impl.functions.AbstractFunctionHandler;
import org.example.sql.function.impl.ColumnELConvertListener;

/**
 * @author chijiuwang
 */
public class NowFunctionHandler extends AbstractFunctionHandler {

    private static final int ARG_SIZE = 0;

    @Override
    public ColumnELConvertListener.TreeNodeInfo processed() {
        if (this.expressionContexts.size() != ARG_SIZE) {
            throw new IllegalArgumentException("NOW():不符合的参数要求");
        }
        return generateTreeNode("CURRENT_TIMESTAMP()", FieldTypeEnum.DATETIME);
    }
}
