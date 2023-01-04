package org.example;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import org.example.enums.FieldTypeEnum;
import org.example.sql.base.OperatorField;
import org.example.sql.function.ColumnELLexer;
import org.example.sql.function.ColumnELParser;
import org.example.sql.function.impl.ColumnELConvertListener;
import org.example.sql.function.impl.ColumnELErrorListener;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chijiuwang@sunlands.com
 */
public class ColumnELTest {
    @Test
    public void test() {

        CharStream charStream = CharStreams.fromString("REPLACE(${金额},2=1,true)");

        ColumnELErrorListener columnELErrorListener = new ColumnELErrorListener();

        ColumnELLexer columnELLexer = new ColumnELLexer(charStream);
        columnELLexer.removeErrorListeners();
        columnELLexer.addErrorListener(columnELErrorListener);
        CommonTokenStream commonTokenStream = new CommonTokenStream(columnELLexer);

        ColumnELParser columnELParser = new ColumnELParser(commonTokenStream);
        columnELParser.removeErrorListeners();
        columnELParser.addErrorListener(columnELErrorListener);
        ParseTree parseTree = columnELParser.expr();

        System.out.println(parseTree);
        System.out.println(parseTree.toStringTree());

        columnELErrorListener.getSyntaxErrors().forEach(System.out::println);

        List<OperatorField> preFields = new ArrayList<>();
        preFields.add(OperatorField.builder().fieldId("金额").sqlExpr(new SQLIdentifierExpr("amount")).fieldType(FieldTypeEnum.NUMBER).build());
        preFields.add(OperatorField.builder().fieldId("流水").sqlExpr(new SQLIdentifierExpr("turnover")).fieldType(FieldTypeEnum.NUMBER).build());
        preFields.add(OperatorField.builder().fieldId("开始时间").sqlExpr(new SQLIdentifierExpr("start_time")).fieldType(FieldTypeEnum.DATE).build());
        preFields.add(OperatorField.builder().fieldId("大区").sqlExpr(new SQLIdentifierExpr("region")).fieldType(FieldTypeEnum.STRING).build());

        ParseTreeWalker walker = new ParseTreeWalker();
        ColumnELConvertListener columnELConvertListener = new ColumnELConvertListener(preFields);
        walker.walk(columnELConvertListener, parseTree);

        System.out.println(columnELConvertListener.getParseTreeProperty().get(parseTree).getRealExpr());
    }
}
