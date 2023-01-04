package org.example.sql.function.impl.functions.impl;

import org.example.enums.FieldTypeEnum;
import org.example.sql.function.impl.functions.AbstractFunctionHandler;
import org.example.sql.function.ColumnELParser;
import org.example.sql.function.impl.ColumnELConvertListener;

/**
 * @author chijiuwang
 */
public class IfFunctionHandler extends AbstractFunctionHandler {

    private static final int ARG_SIZE = 3;

    @Override
    public ColumnELConvertListener.TreeNodeInfo processed() {
        int argSize = this.expressionContexts.size();
        if (argSize != ARG_SIZE) {
            throw new IllegalArgumentException("IF(布尔/数值,参数,参数):不符合的参数要求");
        }
        ColumnELConvertListener.TreeNodeInfo treeNodeInfo1 = getParseTreeNode(this.expressionContexts.get(0)),
                treeNodeInfo2 = getParseTreeNode(this.expressionContexts.get(1)),
                treeNodeInfo3 = getParseTreeNode(this.expressionContexts.get(2));
        if (treeNodeInfo1.getCtx() instanceof ColumnELParser.BooleanExpressionContext ||
                treeNodeInfo1.getCtx() instanceof ColumnELParser.BooleanContext ||
                treeNodeInfo1.getType() == FieldTypeEnum.NUMBER) {
            if (treeNodeInfo2.getType() != treeNodeInfo3.getType()) {
                throw new IllegalArgumentException("IF(布尔/数值,参数,参数):表达式的结果类型必须相同");
            }
            return generateTreeNode(String.format("IF(%s, %s, %s)", treeNodeInfo1.getExpr(), treeNodeInfo2.getRealExpr(), treeNodeInfo3.getRealExpr()), treeNodeInfo2.getType());
        } else {
            throw new RuntimeException("IF(布尔/数值,参数,参数):不符合的参数要求");
        }
    }
}
