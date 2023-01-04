package org.example;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExprGroup;
import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.Collections;

/**
 * @author chijiuwang
 */
public class BaseTests {
    @Test
    public void functionFieldExpr() {
        String funcFormatStr = "DATE_FORMAT(DATE_ADD(%s, INTERVAL -WEEKDAY(%s) DAY),'%%Y-%%m-%%d')";
        int argCount = 2;
        String fieldExpr = "create_time";
        String format = String.format(funcFormatStr, (Object[]) Collections.nCopies(argCount, fieldExpr).toArray(new String[0]));
        System.out.println(format);

        System.out.println(
                formatFieldExprWithFunc(
                        "`T_F71912`.`create_time`",
                        "date(date_add(%s, interval -(((mod(\n" +
                                "    (weekday(date(%s)) + 1), \n" +
                                "    7\n" +
                                "  ) + 1) - 1)) day))",
                        2
                )
        );

        System.out.println(
                formatFieldExprWithFunc(
                        "`T_F71912`.`create_time`",
                        "cast(truncate(((timestampdiff(day, str_to_date(date_format(date(%s), '%%Y-01-01'), '%%Y-%%m-%%d'), date(`T_F71912`.`create_time`)) + 6 + (mod(\n" +
                                "    (weekday(str_to_date(date_format(date(%s), '%%Y-01-01'), '%%Y-%%m-%%d')) + 1), \n" +
                                "    7\n" +
                                "  ) + 1)) / 7), 0) as signed)",
                        2
                )
        );

        System.out.println(
                formatFieldExprWithFunc(
                        "`T_F71912`.`create_time`",
                        "(weekday(date(%s)) + 1)",
                        1
                )
        );

    }

    private String formatFieldExprWithFunc(String fieldExpr, String funcFormatStr, int argCount) {
        return String.format(funcFormatStr, (Object[]) Collections.nCopies(argCount, fieldExpr).toArray(new String[0]));
    }

    @Test
    public void sqlExprTest() {
        SQLBinaryOpExprGroup sqlBinaryOpExprGroup = new SQLBinaryOpExprGroup(SQLBinaryOperator.BooleanAnd);

        sqlBinaryOpExprGroup.add(new SQLIdentifierExpr("`t5`.`__col_12` >= TIMESTAMP '2022-06-07 00:00:00'"));
        sqlBinaryOpExprGroup.add(new SQLIdentifierExpr("`t5`.`__col_12` < TIMESTAMP '2022-06-08 00:00:00'"));
        sqlBinaryOpExprGroup.add(new SQLIdentifierExpr("`t5`.`__col_12` < TIMESTAMP '2022-06-08 00:00:00'"));
        SQLBinaryOpExpr sqlBinaryOpExpr = new SQLBinaryOpExpr(sqlBinaryOpExprGroup, SQLBinaryOperator.BooleanOr, new SQLIdentifierExpr("`t5`.`__col_14` IS NULL"));
        System.out.println(SQLUtils.toSQLString(sqlBinaryOpExpr));
    }

    @Test
    public void isNumber() {
        System.out.println(StringUtils.isNumeric("001"));
    }

}
