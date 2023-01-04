package org.example.sql.function.impl.functions.impl;

import org.example.enums.FieldTypeEnum;
import org.example.sql.function.impl.functions.AbstractFunctionHandler;
import org.example.sql.function.ColumnELParser;
import org.example.sql.function.impl.ColumnELConvertListener;

import java.util.StringJoiner;

/**
 * @author chijiuwang
 */
public class AndFunctionHandler extends AbstractFunctionHandler {

    protected String delimiter = "AND";

    @Override
    public ColumnELConvertListener.TreeNodeInfo processed() {
        if (this.expressionContexts.isEmpty()) {
            return generateTreeNode("1", FieldTypeEnum.NUMBER);
        }
        StringJoiner joiner = new StringJoiner(" \n\t" + this.delimiter + " ", "cast(((\n\t", " \n\t)) as signed)");
        ColumnELConvertListener.TreeNodeInfo expr;
        for (ColumnELParser.ExpressionContext expressionContext : this.expressionContexts) {
            expr = getParseTreeNode(expressionContext);
            if (isLogicalExpression(expr)) {
                joiner.add(isSimpleNumberContext(expr) ? expr.getExpr() + " <> 0" : expr.getExpr());
            } else {
                throw new IllegalArgumentException(this.delimiter + ":参数类型必须为布尔类型或数值类型");
            }
        }
        return generateTreeNode(joiner.toString(), FieldTypeEnum.NUMBER);
    }

    protected boolean isLogicalExpression(ColumnELConvertListener.TreeNodeInfo treeNodeInfo) {
        return treeNodeInfo.getCtx() instanceof ColumnELParser.BooleanExpressionContext ||
                treeNodeInfo.getCtx() instanceof ColumnELParser.BooleanContext ||
                treeNodeInfo.getType() == FieldTypeEnum.NUMBER;
    }

    protected boolean isSimpleNumberContext(ColumnELConvertListener.TreeNodeInfo treeNodeInfo) {
        ColumnELParser.ExpressionContext ctx = treeNodeInfo.getCtx();
        if (ctx instanceof ColumnELParser.NumberContext) {
            return true;
        } else if (ctx instanceof ColumnELParser.ParenExpressionContext) {
            ColumnELParser.ExpressionContext expression = ((ColumnELParser.ParenExpressionContext) ctx).expression();
            return expression instanceof ColumnELParser.NumberContext;
        }
        return false;
    }
}
