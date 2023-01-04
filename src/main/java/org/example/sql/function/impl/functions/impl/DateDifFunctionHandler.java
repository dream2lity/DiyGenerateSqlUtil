package org.example.sql.function.impl.functions.impl;

import org.example.enums.FieldTypeEnum;
import org.example.sql.function.impl.functions.AbstractFunctionHandler;
import org.example.sql.function.impl.ColumnELConvertListener;

/**
 *
 * md (day(`T_694FCE`.`update_time`) - day(`T_694FCE`.`start_date`))
 * y (year(`T_694FCE`.`update_time`) - year(`T_694FCE`.`start_date`))
 * m (((year(`T_694FCE`.`update_time`) - year(`T_694FCE`.`start_date`)) * 12) + (month(`T_694FCE`.`update_time`) - month(`T_694FCE`.`start_date`)))
 * d timestampdiff(day, `T_694FCE`.`start_date`, `T_694FCE`.`update_time`)
 * ym (month(`T_694FCE`.`update_time`) - month(`T_694FCE`.`start_date`))
 * yd (dayofyear(date(`T_694FCE`.`update_time`)) - dayofyear(`T_694FCE`.`start_date`))
 *
 * m (((year(date('2022-01-22')) - year(`T_694FCE`.`start_date`)) * 12) + (month(date('2022-01-22')) - month(`T_694FCE`.`start_date`)))
 * y (year(date('2022-01-22')) - year(`T_694FCE`.`start_date`))
 *
 * @author chijiuwang
 */
public class DateDifFunctionHandler extends AbstractFunctionHandler {

    private static final int ARG_SIZE = 3;

    enum UnitEnum {
        /**
         * 时间差单位
         */
        Y,M,D,MD,YM,YD;
    }

    @Override
    public ColumnELConvertListener.TreeNodeInfo processed() {
        if (this.expressionContexts.size() != ARG_SIZE) {
            throw new IllegalArgumentException("DATEDIF(日期/文本,日期/文本,文本):不符合的参数要求");
        }
        ColumnELConvertListener.TreeNodeInfo startDate = getParseTreeNode(this.expressionContexts.get(0)),
                endDate = getParseTreeNode(this.expressionContexts.get(1)),
                unit = getParseTreeNode(this.expressionContexts.get(2));
        if (isDateOrStringType(startDate.getType()) && isDateOrStringType(endDate.getType()) && isStringType(unit.getType())) {
            UnitEnum unitEnum = UnitEnum.valueOf(getStringLiteral(unit.getExpr()).toUpperCase());
            String startDateExpr = getFinalDate(startDate), endDateExpr = getFinalDate(endDate), expr;
            switch (unitEnum) {
                case Y:
                    expr = String.format("(YEAR(%s) - YEAR(%s))", endDateExpr, startDateExpr);
                    break;
                case M:
                    expr = String.format("(((YEAR(%s) - YEAR(%s)) * 12) + (MONTH(%s) - MONTH(%s)))", endDateExpr, startDateExpr, endDateExpr, startDateExpr);
                    break;
                case D:
                    expr = String.format("TIMESTAMPDIFF(day, %s, %s)", startDateExpr, endDateExpr);
                    break;
                case MD:
                    expr = String.format("(DAY(%s) - DAY(%s))", endDateExpr, startDateExpr);
                    break;
                case YM:
                    expr = String.format("(MONTH(%s) - MONTH(%s))", endDateExpr, startDateExpr);
                    break;
                case YD:
                    expr = String.format("(DAYOFYEAR(%s) - DAYOFYEAR(%s))", endDateExpr, startDateExpr);
                    break;
                default:
                    throw new IllegalArgumentException("DATEDIF(日期/文本,日期/文本,文本):不符合的参数要求");
            }
            return generateTreeNode(expr, FieldTypeEnum.NUMBER);
        } else {
            throw new IllegalArgumentException("DATEDIF(日期/文本,日期/文本,文本):不符合的参数要求");
        }
    }

}
