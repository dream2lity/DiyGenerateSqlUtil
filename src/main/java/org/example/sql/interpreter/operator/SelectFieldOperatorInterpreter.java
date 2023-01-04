package org.example.sql.interpreter.operator;

import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import org.example.enums.TableTypeEnum;
import org.example.sql.OperatorSqlInterpreter;
import org.example.sql.base.Field;
import org.example.sql.base.OperatorCheckResult;
import org.example.sql.interpreter.OperatorInterpreter;
import org.example.sql.operator.SelectFieldOperator;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.stream.Collectors;

import static org.example.sql.SqlBuildHelper.*;

/**
 * @author chijiuwang
 */
public class SelectFieldOperatorInterpreter extends OperatorInterpreter<SelectFieldOperator> {

    public SelectFieldOperatorInterpreter(SelectFieldOperator operator, OperatorSqlInterpreter sqlInterpreter) {
        super(operator, sqlInterpreter);
    }

    @Override
    public void checkOperators() {
        this.sqlInterpreter.getOperatorCheckResults().add(new OperatorCheckResult());

        if (!silentCheckOperator()) {
            return;
        }

        List<Field> fieldList = this.operator.getValue().getFields();
        fieldList.forEach(f ->
                this.sqlInterpreter.addCheckField(convertFieldType(f.getFieldType()), getFieldTransferName(f), null)
        );
    }

    @Override
    public void generateFieldList() {
        List<Field> fieldList = this.operator.getValue().getFields();
        fieldList.forEach(f ->
                this.sqlInterpreter.addField(convertFieldType(f.getFieldType()), getFieldTransferName(f), null)
        );
    }

    @Override
    public void generateSql() {
        SelectFieldOperator.Fields selectFieldValue = this.operator.getValue();
        TableTypeEnum tableTypeEnum = TableTypeEnum.convertByCode(selectFieldValue.getTableType());
        Long tableId = selectFieldValue.getTableId();
        List<Field> fields = selectFieldValue.getFields();
        SQLSelectQueryBlock queryBlock = this.sqlInterpreter.getQueryBlock();
        switch (tableTypeEnum) {
            case DB:
                String tableName = this.sqlInterpreter.getTableName(this.sqlInterpreter.addDbTable(selectFieldValue.getTableName()));
                queryBlock.setFrom(tableNameWithAlias(selectFieldValue.getTableName(), tableName));
                fields.forEach(f -> {
                    String alias = this.sqlInterpreter.nextColumnAlias();
                    SQLPropertyExpr sqlExpr = fullField(tableName, f.getFieldName());
                    queryBlock.addSelectItem(sqlExpr, quote(alias));
                    this.sqlInterpreter.addField(convertFieldType(f.getFieldType()), getFieldTransferName(f), sqlExpr, alias, null);
                });

                this.sqlInterpreter.fillPermissionPlaceHolder(queryBlock, tableId, tableName);

                break;
            case DIY:
                String newTableName = this.sqlInterpreter.nextTableAlias();
                SQLSelectQueryBlock diyTableQuery = this.sqlInterpreter.setDiyTableQuery(tableId);
                queryBlock.setFrom(diyTableQuery, quote(newTableName));
                List<String> selectFieldNames = fields.stream().map(Field::getFieldName).collect(Collectors.toList());
                this.sqlInterpreter.getDiyTableFields().get(tableId).forEach(f -> {
                    if (selectFieldNames.contains(f.getRealTransferName())) {
                        String alias = this.sqlInterpreter.nextColumnAlias();
                        SQLPropertyExpr sqlExpr = fullField(newTableName, f.getAlias());
                        queryBlock.addSelectItem(sqlExpr, quote(alias));
                        this.sqlInterpreter.addField(f.getFieldType(), f.getRealTransferName(), sqlExpr, alias, null);
                    }
                });
                this.sqlInterpreter.setNeedSubQuery(true);
                break;
            default:
                throw new IllegalArgumentException("unsupported table type.");
        }
    }

    @Override
    public void checkOperator() {
        super.checkOperator();
        SelectFieldOperator.Fields value = this.operator.getValue();
        if (CollectionUtils.isEmpty(value.getFields())) {
            throw new IllegalArgumentException("选字段中至少选择一个字段");
        }
    }
}
