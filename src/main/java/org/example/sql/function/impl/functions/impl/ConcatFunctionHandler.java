package org.example.sql.function.impl.functions.impl;

import org.example.enums.FieldTypeEnum;
import org.example.sql.function.impl.functions.AbstractFunctionHandler;
import org.example.sql.function.ColumnELParser;
import org.example.sql.function.impl.ColumnELConvertListener;

import java.util.StringJoiner;

/**
 *
 * concat(
 *             ' Monthly Report',
 *             '1',
 *             '2',
 *             cast((1 + 2) as char),
 *             case when 1 = 4 then '1'
 *                  when not(1 = 4) then '0'
 *                  else null
 *             end,
 *             '1',
 *             date_format(`T_694FCE`.`start_date`,'%Y-%m-%d'),
 *             cast(`T_694FCE`.`amount` as char)
 *           )
 *
 * @author chijiuwang
 */
public class ConcatFunctionHandler extends AbstractFunctionHandler {
    private static final int MIN_ARG_SIZE = 1;

    @Override
    public ColumnELConvertListener.TreeNodeInfo processed() {
        if (this.expressionContexts.size() < MIN_ARG_SIZE) {
            throw new IllegalArgumentException("CONCAT(任意类型...):不符合的参数要求");
        }
        StringJoiner joiner = new StringJoiner(",\n\t", "CONCAT(\n\t", "\n)");
        ColumnELConvertListener.TreeNodeInfo item;
        for (ColumnELParser.ExpressionContext expressionContext : this.expressionContexts) {
            item = getParseTreeNode(expressionContext);
            joiner.add(convertToStringExpression(item));
        }

        return generateTreeNode(joiner.toString(), FieldTypeEnum.STRING);
    }

}
