// Generated from D:/workspace/projects/feature/dc-bi/dc-eagle-model/src/main/resources/antlr\ColumnEL.g4 by ANTLR 4.10.1
package org.example.sql.function;
import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link ColumnELParser}.
 */
public interface ColumnELListener extends ParseTreeListener {
	/**
	 * Enter a parse tree produced by {@link ColumnELParser#expr}.
	 * @param ctx the parse tree
	 */
	void enterExpr(ColumnELParser.ExprContext ctx);
	/**
	 * Exit a parse tree produced by {@link ColumnELParser#expr}.
	 * @param ctx the parse tree
	 */
	void exitExpr(ColumnELParser.ExprContext ctx);
	/**
	 * Enter a parse tree produced by the {@code number}
	 * labeled alternative in {@link ColumnELParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterNumber(ColumnELParser.NumberContext ctx);
	/**
	 * Exit a parse tree produced by the {@code number}
	 * labeled alternative in {@link ColumnELParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitNumber(ColumnELParser.NumberContext ctx);
	/**
	 * Enter a parse tree produced by the {@code identifier}
	 * labeled alternative in {@link ColumnELParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterIdentifier(ColumnELParser.IdentifierContext ctx);
	/**
	 * Exit a parse tree produced by the {@code identifier}
	 * labeled alternative in {@link ColumnELParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitIdentifier(ColumnELParser.IdentifierContext ctx);
	/**
	 * Enter a parse tree produced by the {@code boolean}
	 * labeled alternative in {@link ColumnELParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterBoolean(ColumnELParser.BooleanContext ctx);
	/**
	 * Exit a parse tree produced by the {@code boolean}
	 * labeled alternative in {@link ColumnELParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitBoolean(ColumnELParser.BooleanContext ctx);
	/**
	 * Enter a parse tree produced by the {@code string}
	 * labeled alternative in {@link ColumnELParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterString(ColumnELParser.StringContext ctx);
	/**
	 * Exit a parse tree produced by the {@code string}
	 * labeled alternative in {@link ColumnELParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitString(ColumnELParser.StringContext ctx);
	/**
	 * Enter a parse tree produced by the {@code additiveExpression}
	 * labeled alternative in {@link ColumnELParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterAdditiveExpression(ColumnELParser.AdditiveExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code additiveExpression}
	 * labeled alternative in {@link ColumnELParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitAdditiveExpression(ColumnELParser.AdditiveExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code parenExpression}
	 * labeled alternative in {@link ColumnELParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterParenExpression(ColumnELParser.ParenExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code parenExpression}
	 * labeled alternative in {@link ColumnELParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitParenExpression(ColumnELParser.ParenExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code multiplicativeExpression}
	 * labeled alternative in {@link ColumnELParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterMultiplicativeExpression(ColumnELParser.MultiplicativeExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code multiplicativeExpression}
	 * labeled alternative in {@link ColumnELParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitMultiplicativeExpression(ColumnELParser.MultiplicativeExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code booleanExpression}
	 * labeled alternative in {@link ColumnELParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterBooleanExpression(ColumnELParser.BooleanExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code booleanExpression}
	 * labeled alternative in {@link ColumnELParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitBooleanExpression(ColumnELParser.BooleanExpressionContext ctx);
	/**
	 * Enter a parse tree produced by the {@code functionExpression}
	 * labeled alternative in {@link ColumnELParser#expression}.
	 * @param ctx the parse tree
	 */
	void enterFunctionExpression(ColumnELParser.FunctionExpressionContext ctx);
	/**
	 * Exit a parse tree produced by the {@code functionExpression}
	 * labeled alternative in {@link ColumnELParser#expression}.
	 * @param ctx the parse tree
	 */
	void exitFunctionExpression(ColumnELParser.FunctionExpressionContext ctx);
}