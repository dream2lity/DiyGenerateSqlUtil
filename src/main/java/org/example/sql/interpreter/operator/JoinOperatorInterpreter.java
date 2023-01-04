package org.example.sql.interpreter.operator;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOpExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import org.example.enums.JoinTypeEnum;
import org.example.enums.TableTypeEnum;
import org.example.sql.OperatorSqlInterpreter;
import org.example.sql.base.Field;
import org.example.sql.base.OperatorCheckResult;
import org.example.sql.base.OperatorField;
import org.example.sql.interpreter.OperatorInterpreter;
import org.example.sql.operator.JoinOperator;
import org.example.sql.operator.SelectFieldOperator;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.example.sql.SqlBuildHelper.*;

/**
 * @author chijiuwang
 */
public class JoinOperatorInterpreter extends OperatorInterpreter<JoinOperator> {

    private Map<Long, Field> columnIdFieldMapping = new HashMap<>();

    public JoinOperatorInterpreter(JoinOperator operator, OperatorSqlInterpreter sqlInterpreter) {
        super(operator, sqlInterpreter);
    }

    @Override
    public void checkOperators() {
        OperatorCheckResult checkResult = new OperatorCheckResult();
        this.sqlInterpreter.getOperatorCheckResults().add(checkResult);

        if (!silentCheckOperator()) {
            return;
        }

        List<List<String>> basis = this.operator.getValue().getBasis();
        List<String> preFieldIds = this.sqlInterpreter.getCheckFields().stream().map(OperatorField::getFieldId).collect(Collectors.toList());
        List<Field> fieldList = this.operator.getValue().getTable().getFields();
        basis.forEach(b -> {
            if (Objects.nonNull(b.get(0)) && !preFieldIds.contains(b.get(0))) {
                OperatorCheckResult.MissFieldInfo missFieldInfo = new OperatorCheckResult.MissFieldInfo();
                missFieldInfo.setFieldId(b.get(0));
                checkResult.addMissFieldInfo(missFieldInfo);
            }
            if (Objects.nonNull(b.get(1)) && fieldList.stream().noneMatch(f -> Long.parseLong(b.get(1)) == f.getColumnId())) {
                OperatorCheckResult.MissFieldInfo missFieldInfo = new OperatorCheckResult.MissFieldInfo();
                missFieldInfo.setFieldId(b.get(1));
                checkResult.addMissFieldInfo(missFieldInfo);
            }
        });

        fieldList.forEach(f ->
                this.sqlInterpreter.addCheckField(convertFieldType(f.getFieldType()), getFieldTransferName(f), null)
        );
    }

    @Override
    public void generateFieldList() {
        List<Field> fieldList = this.operator.getValue().getTable().getFields();
        fieldList.forEach(f ->
                this.sqlInterpreter.addField(convertFieldType(f.getFieldType()), getFieldTransferName(f), null)
        );
    }

    @Override
    public void generateSql() {
        JoinOperator.JoinInfo joinValue = this.operator.getValue();

        SelectFieldOperator.Fields joinTable = joinValue.getTable();

        TableTypeEnum tableTypeEnum = TableTypeEnum.convertByCode(joinTable.getTableType());
        switch (tableTypeEnum) {
            case DB:
                joinDb(this.sqlInterpreter.getQueryBlock(), joinValue);
                break;
            case DIY:
                joinDiy(this.sqlInterpreter.getQueryBlock(), joinValue);
                break;
            default:
                throw new IllegalArgumentException("unsupported table type.");
        }

        this.sqlInterpreter.setNeedSubQuery(true);
    }

    @Override
    public void checkOperator() {
        super.checkOperator();
        JoinOperator.JoinInfo value = this.operator.getValue();
        if (CollectionUtils.isEmpty(value.getBasis()) || Objects.isNull(value.getBasis().get(0).get(0)) || Objects.isNull(value.getBasis().get(0).get(1))) {
            throw new IllegalArgumentException("左右合并中至少选择一个合并依据");
        }
        if (Objects.isNull(value.getTable()) || CollectionUtils.isEmpty(value.getTable().getFields())) {
            throw new IllegalArgumentException("左右合并中至少选择一个字段");
        }
    }

    private void subQueryJoin(SQLSelectQueryBlock queryBlock, JoinOperator.JoinInfo joinValue,
                              Function<String, SQLPropertyExpr> joinTableFieldFunction,
                              Supplier<? extends SQLTableSource> joinRightTableSupplier, String placeholderPermissionTableName) {
        JoinTypeEnum joinTypeEnum = JoinTypeEnum.convertByCode(joinValue.getStyle());

        SelectFieldOperator.Fields joinTable = joinValue.getTable();
        SQLSelectQueryBlock oldQueryBlock = new SQLSelectQueryBlock();
        queryBlock.cloneTo(oldQueryBlock);

        SQLSelectQueryBlock newQueryBlock = new SQLSelectQueryBlock();
        String newTableName = this.sqlInterpreter.nextTableAlias();

        List<List<String>> basis = joinValue.getBasis();

        Map<String, OperatorField> preAliasFieldMapping = this.sqlInterpreter.aliasFieldMapping();
        oldQueryBlock.getSelectList().forEach(f -> {
            String alias = this.sqlInterpreter.nextColumnAlias();
            String preAlias = unQuote(f.getAlias());
            SQLPropertyExpr sqlExpr = fullField(newTableName, preAlias);
            newQueryBlock.addSelectItem(sqlExpr, quote(alias));
            OperatorField preField = preAliasFieldMapping.get(preAlias);
            preField.setSqlExpr(sqlExpr);
            preField.setAlias(alias);
        });
        List<OperatorField> leftTableFields = new ArrayList<>(this.sqlInterpreter.getFields());
        joinTable.getFields().forEach(f -> {
            String alias = this.sqlInterpreter.nextColumnAlias();
            SQLPropertyExpr sqlExpr = joinTableFieldFunction.apply(f.getFieldName());
            newQueryBlock.addSelectItem(sqlExpr, quote(alias));
            this.sqlInterpreter.addField(convertFieldType(f.getFieldType()), getFieldTransferName(f), sqlExpr, alias, null);
        });

        List<SQLBinaryOpExpr> joinConditions = basis.stream().map(c ->
                joinCondition(
                        findSourceField(c.get(0), leftTableFields).getSqlExpr(),
                        joinTableFieldFunction.apply(getFieldNameByColumnId(Long.valueOf(c.get(1)), joinValue)))
        ).collect(Collectors.toList());

        SQLJoinTableSource joinTableSource = new SQLJoinTableSource();
        joinTableSource.setJoinType(SQLJoinTableSource.JoinType.valueOf(joinTypeEnum.name()));
        joinTableSource.setLeft(tableWithAlias(oldQueryBlock, newTableName));
        joinTableSource.setRight(joinRightTableSupplier.get());

        joinTableSource.setCondition(multiJoinCondition(joinConditions));

        newQueryBlock.setFrom(joinTableSource);

        if (placeholderPermissionTableName != null) {
            this.sqlInterpreter.fillPermissionPlaceHolder(newQueryBlock, joinTable.getTableId(), placeholderPermissionTableName);
        }

        queryBlock.setWhere(null);
        queryBlock.setGroupBy(null);
        newQueryBlock.cloneTo(queryBlock);

        this.sqlInterpreter.setFirstJoin(false);
    }

    private void joinDiy(SQLSelectQueryBlock queryBlock, JoinOperator.JoinInfo joinValue) {
        JoinTypeEnum joinTypeEnum = JoinTypeEnum.convertByCode(joinValue.getStyle());

        SelectFieldOperator.Fields joinTable = joinValue.getTable();
        if (this.sqlInterpreter.isNeedSubQuery()) {
            String diyTableNewTableName = this.sqlInterpreter.nextTableAlias();
            SQLSelectQueryBlock diyTableQuery = this.sqlInterpreter.setDiyTableQuery(joinTable.getTableId());
            Map<String, OperatorField> fieldInfoMap = this.sqlInterpreter.getDiyTableFields().get(joinTable.getTableId()).stream().collect(Collectors.toMap(
                    OperatorField::getRealTransferName,
                    f -> f, (f1, f2) -> f1));

            subQueryJoin(
                    queryBlock,
                    joinValue,
                    fieldName -> fullField(diyTableNewTableName, fieldInfoMap.get(fieldName).getAlias()),
                    () -> tableWithAlias(diyTableQuery, diyTableNewTableName),
                    null
            );

        } else {
            String newTableName = this.sqlInterpreter.nextTableAlias();
            SQLSelectQueryBlock diyTableQuery = this.sqlInterpreter.setDiyTableQuery(joinTable.getTableId());
            List<String> selectFieldNames = joinTable.getFields().stream().map(Field::getFieldName).collect(Collectors.toList());
            List<OperatorField> leftTableFields = new ArrayList<>(this.sqlInterpreter.getFields());
            this.sqlInterpreter.getDiyTableFields().get(joinTable.getTableId()).forEach(f -> {
                if (selectFieldNames.contains(f.getRealTransferName())) {
                    String alias = this.sqlInterpreter.nextColumnAlias();
                    SQLPropertyExpr sqlExpr = fullField(newTableName, f.getAlias());
                    queryBlock.addSelectItem(sqlExpr, quote(alias));
                    this.sqlInterpreter.addField(f.getFieldType(), f.getRealTransferName(), sqlExpr, alias, null);
                }
            });

            SQLJoinTableSource joinTableSource = new SQLJoinTableSource();
            joinTableSource.setJoinType(SQLJoinTableSource.JoinType.valueOf(joinTypeEnum.name()));
            joinTableSource.setLeft(queryBlock.getFrom());
            joinTableSource.setRight(tableWithAlias(diyTableQuery, newTableName));

            Map<String, OperatorField> fieldInfoMap = this.sqlInterpreter.getDiyTableFields().get(joinTable.getTableId()).stream()
                    .collect(Collectors.toMap(OperatorField::getRealTransferName, f -> f, (f1, f2) -> f1));
            List<List<String>> basis = joinValue.getBasis();
            List<SQLBinaryOpExpr> joinConditions = basis.stream().map(c ->
                    joinCondition(
                            findSourceField(c.get(0), leftTableFields).getSqlExpr(),
                            fullField(newTableName, fieldInfoMap.get(getFieldNameByColumnId(Long.valueOf(c.get(1)), joinValue)).getAlias()))
            ).collect(Collectors.toList());
            joinTableSource.setCondition(multiJoinCondition(joinConditions));

            queryBlock.setFrom(joinTableSource);

            this.sqlInterpreter.setNeedSubQuery(true);
        }
    }

    private void joinDb(SQLSelectQueryBlock queryBlock, JoinOperator.JoinInfo joinValue) {
        JoinTypeEnum joinTypeEnum = JoinTypeEnum.convertByCode(joinValue.getStyle());

        SelectFieldOperator.Fields joinTable = joinValue.getTable();
        String joinTableName = this.sqlInterpreter.getTableName(this.sqlInterpreter.addDbTable(joinTable.getTableName()));

        if (this.sqlInterpreter.isNeedSubQuery()) {

            subQueryJoin(
                    queryBlock,
                    joinValue,
                    fieldName -> fullField(joinTableName, fieldName),
                    () -> tableNameWithAlias(joinTable.getTableName(), joinTableName),
                    joinTableName
            );

        } else {
            List<OperatorField> leftTableFields = new ArrayList<>(this.sqlInterpreter.getFields());
            joinTable.getFields().forEach(f -> {
                SQLPropertyExpr sqlExpr = fullField(joinTableName, f.getFieldName());
                String alias = this.sqlInterpreter.nextColumnAlias();
                queryBlock.addSelectItem(sqlExpr, quote(alias));
                this.sqlInterpreter.addField(convertFieldType(f.getFieldType()), getFieldTransferName(f), sqlExpr, alias, null);
            });

            SQLJoinTableSource joinTableSource = new SQLJoinTableSource();
            joinTableSource.setJoinType(SQLJoinTableSource.JoinType.valueOf(joinTypeEnum.name()));
            joinTableSource.setLeft(queryBlock.getFrom());
            joinTableSource.setRight(tableNameWithAlias(joinTable.getTableName(), joinTableName));

            List<List<String>> basis = joinValue.getBasis();
            List<SQLBinaryOpExpr> joinConditions = basis.stream().map(c ->
                    joinCondition(
                            findSourceField(c.get(0), leftTableFields).getSqlExpr(),
                            fullField(joinTableName, getFieldNameByColumnId(Long.valueOf(c.get(1)), joinValue)))
            ).collect(Collectors.toList());
            joinTableSource.setCondition(multiJoinCondition(joinConditions));

            queryBlock.setFrom(joinTableSource);

            this.sqlInterpreter.fillPermissionPlaceHolder(queryBlock, joinTable.getTableId(), joinTableName);

            this.sqlInterpreter.setFirstJoin(true);
        }
    }

    private String getFieldNameByColumnId(Long columnId, JoinOperator.JoinInfo joinValue) {
        if (CollectionUtils.isEmpty(this.columnIdFieldMapping)) {
            List<Field> fields = joinValue.getTable().getFields();
            this.columnIdFieldMapping = fields.stream().collect(Collectors.toMap(Field::getColumnId, f -> f, (o1, o2) -> o1));
        }
        return this.columnIdFieldMapping.get(columnId).getFieldName();
    }
}
