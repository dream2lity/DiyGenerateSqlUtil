package org.example.sql.interpreter.operator;

import org.example.sql.OperatorSqlInterpreter;
import org.example.sql.interpreter.OperatorInterpreter;
import org.example.sql.operator.AddFieldOperator;

/**
 * @author chijiuwang@sunlands.com
 */
public class AddFieldOperatorInterpreter extends OperatorInterpreter<AddFieldOperator> {

    public AddFieldOperatorInterpreter(AddFieldOperator operator, OperatorSqlInterpreter sqlInterpreter) {
        super(operator, sqlInterpreter);
    }

    @Override
    public void checkOperators() {
        operator.getValue().forEach(addFieldInfo -> {
            if (addFieldInfo instanceof AddFieldOperator.AddFunctionFieldInfo) {

            } else if (addFieldInfo instanceof AddFieldOperator.AddGroupByFieldInfo) {

            } else if (addFieldInfo instanceof AddFieldOperator.AddDivideNumberDiyFieldInfo) {

            } else if (addFieldInfo instanceof AddFieldOperator.AddDivideTextFieldInfo) {

            }
            throw new IllegalArgumentException("不支持的新增列类型");
        });
    }

    @Override
    public void generateFieldList() {

    }

    @Override
    public void generateSql() {

    }
}
