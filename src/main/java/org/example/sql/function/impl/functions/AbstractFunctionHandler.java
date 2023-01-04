package org.example.sql.function.impl.functions;

import org.example.enums.FieldTypeEnum;
import org.antlr.v4.runtime.tree.ParseTree;

import java.util.List;
import java.util.function.Function;

import static org.example.sql.function.ColumnELParser.*;
import static org.example.sql.function.impl.ColumnELConvertListener.*;

/**
 * @author chijiuwang
 */
public abstract class AbstractFunctionHandler {
    protected FunctionExpressionContext ctx;
    protected List<ExpressionContext> expressionContexts;
    protected Function<ParseTree, TreeNodeInfo> treeTreeNodeInfoFunction;

    public void setCtx(FunctionExpressionContext ctx) {
        this.ctx = ctx;
        this.expressionContexts = ctx.expression();
    }

    public void setTreeTreeNodeInfoFunction(Function<ParseTree, TreeNodeInfo> treeTreeNodeInfoFunction) {
        this.treeTreeNodeInfoFunction = treeTreeNodeInfoFunction;
    }

    protected TreeNodeInfo getParseTreeNode(ParseTree node) {
        return this.treeTreeNodeInfoFunction.apply(node);
    }

    /**
     * 解析、转换
     * @return 处理结果
     */
    public abstract TreeNodeInfo processed();

    protected int argSize = 0;
    protected TreeNodeInfo[] args;
    protected FieldTypeEnum[] argTypes;
    protected String illegalArgumentMessage = "不符合的参数要求";
    protected void doProcess() {
        prepareArguments();
        if (this.expressionContexts.size() != argSize) {
            throw new IllegalArgumentException(illegalArgumentMessage);
        }

    }

    protected void prepareArguments() {
        args = new TreeNodeInfo[argSize];
        for (int i = 0; i < argSize; i++) {
            args[i] = getParseTreeNode(this.expressionContexts.get(i));
        }
    }

    protected TreeNodeInfo generateTreeNode(String expr, FieldTypeEnum type) {
        return new TreeNodeInfo(expr, this.ctx, type);
    }

    protected String getStringLiteral(String expr) {
        return expr.substring(1, expr.length() - 1);
    }

    protected String getFinalDate(TreeNodeInfo date) {
        return isStringType(date.getType()) ? String.format("date(%s)", date.getExpr()) : date.getExpr();
    }

    protected boolean isDateType(FieldTypeEnum typeEnum) {
        return typeEnum == FieldTypeEnum.DATE || typeEnum == FieldTypeEnum.DATETIME;
    }

    protected boolean isSimpleDateType(FieldTypeEnum typeEnum) {
        return typeEnum == FieldTypeEnum.DATE;
    }

    protected boolean isSimpleDateTimeType(FieldTypeEnum typeEnum) {
        return typeEnum == FieldTypeEnum.DATETIME;
    }

    protected boolean isStringType(FieldTypeEnum typeEnum) {
        return typeEnum == FieldTypeEnum.STRING;
    }

    protected boolean isNumberType(FieldTypeEnum typeEnum) {
        return typeEnum == FieldTypeEnum.NUMBER;
    }

    protected boolean isDateOrStringType(FieldTypeEnum typeEnum) {
        return isDateType(typeEnum) || isStringType(typeEnum);
    }

    protected String convertToStringExpression(TreeNodeInfo treeNodeInfo) {
        if (isStringType(treeNodeInfo.getType())) {
            return treeNodeInfo.getExpr();
        } else if (treeNodeInfo.isSimpleNumber()) {
            return String.format("'%s'", treeNodeInfo.getExpr());
        } else if (treeNodeInfo.isSimpleBoolean()) {
            return String.format("CASE WHEN %s THEN '1'\n" +
                    "       WHEN NOT(%s) THEN '0'\n" +
                    "       ELSE NULL\n" +
                    "    END", treeNodeInfo.getExpr(), treeNodeInfo.getExpr());
        } else if (isSimpleDateType(treeNodeInfo.getType())) {
            return String.format("DATE_FORMAT(%s, '%%Y-%%m-%%d')", treeNodeInfo.getExpr());
        } else if (isSimpleDateTimeType(treeNodeInfo.getType())) {
            return String.format("DATE_FORMAT(%s,'%%Y-%%m-%%d %%H:%%i:%%s')", treeNodeInfo.getExpr());
        } else if (isNumberType(treeNodeInfo.getType())) {
            return String.format("CAST((%s) AS CHAR)", treeNodeInfo.getExpr());
        }
        throw new IllegalArgumentException("convert to String expression failed.");
    }

}
