package org.example.sql.interpreter;

import org.example.sql.OperatorSqlInterpreter;
import org.example.sql.interpreter.operator.*;
import org.example.sql.operator.*;

/**
 * @author chijiuwang
 */
public class OperatorInterpreterFactory {
    public static OperatorInterpreter<?> getInterpreter(AnalysisOperator<?> operator, OperatorSqlInterpreter sqlInterpreter) {
        if (operator instanceof SelectFieldOperator) {
            return new SelectFieldOperatorInterpreter((SelectFieldOperator) operator, sqlInterpreter);
        } else if (operator instanceof JoinOperator) {
            return new JoinOperatorInterpreter((JoinOperator) operator, sqlInterpreter);
        } else if (operator instanceof SetFieldOperator) {
            return new SetFieldOperatorInterpreter((SetFieldOperator) operator, sqlInterpreter);
        } else if (operator instanceof GroupByOperator) {
            return new GroupByOperatorInterpreter((GroupByOperator) operator, sqlInterpreter);
        } else if (operator instanceof FilterOperator) {
            return new FilterOperatorInterpreter((FilterOperator) operator, sqlInterpreter);
        } else if (operator instanceof AddFieldOperator) {
            return new AddFieldOperatorInterpreter((AddFieldOperator) operator, sqlInterpreter);
        }
        throw new IllegalArgumentException("unsupported operator type.");
    }
}
