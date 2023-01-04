package org.example.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.sql.base.Field;
import org.example.sql.operator.AnalysisOperator;
import org.example.sql.operator.JoinOperator;
import org.example.sql.operator.SelectFieldOperator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * 转换表名及列名称
 * @author chijiuwang
 */
public class TableTransferOperatorSqlProxy extends OperatorSqlProxy {

    protected Map<Long, String> physicsTableNameCache = new HashMap<>();
    protected Map<Long, Map<Long, String>> physicsFieldNameCache = new HashMap<>();

    protected Function<Long, String> getPhysicsTableName;
    protected Function<Long, Map<Long, String>> getPhysicsFieldName;

    protected Function<Long, List<AnalysisOperator<?>>> getTableOperators;

    public TableTransferOperatorSqlProxy(List<AnalysisOperator<?>> operators, ObjectMapper objectMapper,
                                         Function<Long, String> getPhysicsTableName,
                                         Function<Long, Map<Long, String>> getPhysicsFieldName) {
        super(operators, objectMapper);
        this.getPhysicsTableName = getPhysicsTableName;
        this.getPhysicsFieldName = getPhysicsFieldName;
        transferTableAndFields(operators);
    }

    public TableTransferOperatorSqlProxy(List<AnalysisOperator<?>> operators, ObjectMapper objectMapper,
                                         Function<Long, List<AnalysisOperator<?>>> getTableOperators,
                                         Function<Long, String> getPhysicsTableName,
                                         Function<Long, Map<Long, String>> getPhysicsFieldName) {
        super(operators, objectMapper, null);
        this.getPhysicsTableName = getPhysicsTableName;
        this.getPhysicsFieldName = getPhysicsFieldName;
        transferTableAndFields(operators);
        this.getTableOperators = getTableOperators;
        this.operatorSqlInterpreter.setGetTableOperators(this::transferGetTableOperators);
    }

    protected List<AnalysisOperator<?>> transferGetTableOperators(Long tableId) {
        List<AnalysisOperator<?>> operators = this.getTableOperators.apply(tableId);
        transferTableAndFields(operators);
        return operators;
    }

    protected void transferTableAndFields(List<AnalysisOperator<?>> operators) {
        for (AnalysisOperator<?> operator : operators) {
            if (operator instanceof SelectFieldOperator) {
                SelectFieldOperator selectFieldOperator = (SelectFieldOperator) operator;
                transferNames(selectFieldOperator.getValue());
            } else if (operator instanceof JoinOperator) {
                JoinOperator joinOperator = (JoinOperator) operator;
                transferNames(joinOperator.getValue().getTable());
            }
        }
    }

    private void transferNames(SelectFieldOperator.Fields table) {
        table.setTableName(getTableName(table.getTableId()));
        for (Field field : table.getFields()) {
            field.setAliasFieldName(field.getFieldName());
            field.setFieldName(getFieldName(table.getTableId(), field.getColumnId()));
        }
    }

    protected String getTableName(Long tableId) {
        if (!physicsTableNameCache.containsKey(tableId)) {
            if (Objects.isNull(this.getPhysicsTableName)) {
                throw new IllegalArgumentException("Function getPhysicsTableName is not set.");
            }
            physicsTableNameCache.put(tableId, this.getPhysicsTableName.apply(tableId));
        }
        return physicsTableNameCache.get(tableId);
    }

    protected String getFieldName(Long tableId, Long columnId) {
        if (!physicsFieldNameCache.containsKey(tableId)) {
            if (Objects.isNull(this.getPhysicsFieldName)) {
                throw new IllegalArgumentException("Function getPhysicsFieldName is not set.");
            }
            physicsFieldNameCache.put(tableId, this.getPhysicsFieldName.apply(tableId));
        }
        return Objects.requireNonNull(physicsFieldNameCache.get(tableId).get(columnId), "请检查依赖表是否发生变化");
    }
}
