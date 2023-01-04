package org.example.sql.function.impl;

import org.example.enums.FieldTypeEnum;
import org.example.sql.base.OperatorField;
import org.example.sql.function.ColumnELBaseListener;
import org.example.sql.function.impl.functions.AbstractFunctionHandler;
import org.example.sql.function.impl.functions.FunctionFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.antlr.v4.runtime.tree.ParseTreeProperty;
import org.example.sql.function.ColumnELParser;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author chijiuwang
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class ColumnELConvertListener extends ColumnELBaseListener {
    ParseTreeProperty<TreeNodeInfo> parseTreeProperty = new ParseTreeProperty<>();
    Map<String, OperatorField> preFieldTypeMaps;

    public ColumnELConvertListener(List<OperatorField> preFields) {
        this.preFieldTypeMaps = Objects.nonNull(preFields) && !preFields.isEmpty() ?
                preFields.stream().collect(Collectors.toMap(OperatorField::getFieldId, Function.identity())) :
                Collections.emptyMap();
    }

    @Override
    public void exitNumber(ColumnELParser.NumberContext ctx) {
        this.parseTreeProperty.put(ctx, new TreeNodeInfo(ctx.getText(), ctx, FieldTypeEnum.NUMBER));
    }

    @Override
    public void exitBoolean(ColumnELParser.BooleanContext ctx) {
        this.parseTreeProperty.put(ctx, new TreeNodeInfo(ctx.getText(), ctx, FieldTypeEnum.NUMBER));
    }

    @Override
    public void exitString(ColumnELParser.StringContext ctx) {
        this.parseTreeProperty.put(ctx, new TreeNodeInfo(ctx.getText().replaceAll("\"", "'"), ctx, FieldTypeEnum.STRING));
    }

    @Override
    public void exitIdentifier(ColumnELParser.IdentifierContext ctx) {
        String ctxText = ctx.getText();
        String identifierName = ctxText.substring(2, ctxText.length() - 1);
        // TODO: 转换为当前字段名称
        if (!this.preFieldTypeMaps.containsKey(identifierName)) {
            throw new IllegalArgumentException("field [" + identifierName + "] was not found.");
        }
        this.parseTreeProperty.put(ctx,
                new TreeNodeInfo(
                        preFieldTypeMaps.get(identifierName).getSqlExpr().toString(), ctx,
                        preFieldTypeMaps.get(identifierName).getFieldType()));
    }

    @Override
    public void exitBooleanExpression(ColumnELParser.BooleanExpressionContext ctx) {
        String expr1 = this.parseTreeProperty.get(ctx.expression(0)).getExpr();
        String expr2 = this.parseTreeProperty.get(ctx.expression(1)).getExpr();
        String finalExpr;
        switch (ctx.op.getType()) {
            case ColumnELParser.Equal:
                finalExpr = String.format("%s = %s", expr1, expr2);
                break;
            case ColumnELParser.NotEqual:
                finalExpr = String.format("%s != %s", expr1, expr2);
                break;
            case ColumnELParser.Less:
                finalExpr = String.format("%s < %s", expr1, expr2);
                break;
            case ColumnELParser.LessEqual:
                finalExpr = String.format("%s <= %s", expr1, expr2);
                break;
            case ColumnELParser.Greater:
                finalExpr = String.format("%s > %s", expr1, expr2);
                break;
            case ColumnELParser.GreaterEqual:
                finalExpr = String.format("%s >= %s", expr1, expr2);
                break;
            default:
                throw new RuntimeException("unsupported operator.");
        }
        this.parseTreeProperty.put(ctx, new TreeNodeInfo(finalExpr, ctx, FieldTypeEnum.NUMBER));
    }

    @Override
    public void exitMultiplicativeExpression(ColumnELParser.MultiplicativeExpressionContext ctx) {
        String expr1 = this.parseTreeProperty.get(ctx.expression(0)).getExpr();
        String expr2 = this.parseTreeProperty.get(ctx.expression(1)).getExpr();
        String finalExpr;
        switch (ctx.op.getType()) {
            case ColumnELParser.MUL:
                finalExpr = String.format("%s * %s", expr1, expr2);
                break;
            case ColumnELParser.DIV:
                finalExpr = String.format("%s / %s", expr1, expr2);
                break;
            default:
                throw new RuntimeException("unsupported operator.");
        }
        this.parseTreeProperty.put(ctx, new TreeNodeInfo(finalExpr, ctx, FieldTypeEnum.NUMBER));
    }

    @Override
    public void exitAdditiveExpression(ColumnELParser.AdditiveExpressionContext ctx) {
        String expr1 = this.parseTreeProperty.get(ctx.expression(0)).getExpr();
        String expr2 = this.parseTreeProperty.get(ctx.expression(1)).getExpr();
        String finalExpr;
        switch (ctx.op.getType()) {
            case ColumnELParser.ADD:
                finalExpr = String.format("%s + %s", expr1, expr2);
                break;
            case ColumnELParser.SUB:
                finalExpr = String.format("%s - %s", expr1, expr2);
                break;
            default:
                throw new RuntimeException("unsupported operator.");
        }
        this.parseTreeProperty.put(ctx, new TreeNodeInfo(finalExpr, ctx, FieldTypeEnum.NUMBER));
    }

    @Override
    public void exitParenExpression(ColumnELParser.ParenExpressionContext ctx) {
        String expr = ctx.expression() instanceof ColumnELParser.ParenExpressionContext ?
                String.format("%s", this.parseTreeProperty.get(ctx.expression()).getExpr()) :
                String.format("(%s)", this.parseTreeProperty.get(ctx.expression()).getExpr());
        this.parseTreeProperty.put(ctx,
                new TreeNodeInfo(expr, ctx,
                        this.parseTreeProperty.get(ctx.expression()).getType()));
    }

    @Override
    public void exitFunctionExpression(ColumnELParser.FunctionExpressionContext ctx) {
        try {
            AbstractFunctionHandler functionHandler = FunctionFactory.newInstance(ctx, this.parseTreeProperty::get);
            this.parseTreeProperty.put(ctx, functionHandler.processed());
        } catch (InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void exitExpr(ColumnELParser.ExprContext ctx) {
        this.parseTreeProperty.put(ctx, this.parseTreeProperty.get(ctx.expression()));
    }

    @AllArgsConstructor
    @Data
    public static class TreeNodeInfo {
        String expr;
        ColumnELParser.ExpressionContext ctx;
        FieldTypeEnum type;

        public String getRealExpr() {
            if (this.ctx instanceof ColumnELParser.BooleanContext ||
                    this.ctx instanceof ColumnELParser.BooleanExpressionContext) {
                return String.format("cast((%s) as signed)", this.expr);
            } else if (this.ctx instanceof ColumnELParser.ParenExpressionContext) {
                ColumnELParser.ExpressionContext expressionContext = ((ColumnELParser.ParenExpressionContext) this.ctx).expression();
                if (expressionContext instanceof ColumnELParser.BooleanContext || expressionContext instanceof ColumnELParser.BooleanExpressionContext) {
                    return String.format("cast(%s as signed)", this.expr);
                }
            }
            return this.expr;
        }

        public boolean isSimpleBoolean() {
            return this.ctx instanceof ColumnELParser.ParenExpressionContext
                    ? ((ColumnELParser.ParenExpressionContext) this.ctx).expression() instanceof ColumnELParser.BooleanContext || ((ColumnELParser.ParenExpressionContext) this.ctx).expression() instanceof ColumnELParser.BooleanExpressionContext
                    : this.ctx instanceof ColumnELParser.BooleanContext || this.ctx instanceof ColumnELParser.BooleanExpressionContext;
        }

        public boolean isSimpleString() {
            return this.ctx instanceof ColumnELParser.ParenExpressionContext
                    ? ((ColumnELParser.ParenExpressionContext) this.ctx).expression() instanceof ColumnELParser.StringContext
                    : this.ctx instanceof ColumnELParser.StringContext;
        }

        public boolean isSimpleNumber() {
            return this.ctx instanceof ColumnELParser.ParenExpressionContext
                    ? ((ColumnELParser.ParenExpressionContext) this.ctx).expression() instanceof ColumnELParser.NumberContext
                    : this.ctx instanceof ColumnELParser.NumberContext;
        }
    }
}
