package org.example.sql.interpreter;

import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import org.example.enums.FieldTypeEnum;
import org.example.sql.OperatorSqlInterpreter;
import org.example.sql.base.Field;
import org.example.sql.base.OperatorField;
import org.example.sql.operator.AnalysisOperator;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.example.sql.SqlBuildHelper.*;

/**
 * @author chijiuwang
 */
@Slf4j
public abstract class OperatorInterpreter<T extends AnalysisOperator<?>> {
    protected T operator;
    protected OperatorSqlInterpreter sqlInterpreter;

    public OperatorInterpreter(T operator, OperatorSqlInterpreter sqlInterpreter) {
        this.operator = operator;
        this.sqlInterpreter = sqlInterpreter;
    }

    public abstract void checkOperators();
    public abstract void generateFieldList();
    public abstract void generateSql();

    protected FieldTypeEnum convertFieldType(Integer fieldType) {
        return FieldTypeEnum.convertByCode(fieldType);
    }

    protected OperatorField findSourceField(String fieldId, List<OperatorField> fields) {
        return fields.stream().filter(f -> fieldId.equals(f.getFieldId())).findFirst().orElseThrow(() -> new IllegalArgumentException("fieldId " + fieldId + " not found."));
    }

    protected String getFieldTransferName(Field field) {
        return StringUtils.hasText(field.getAliasFieldName()) ? field.getAliasFieldName() : field.getFieldName();
    }

    protected void generateSubQuery() {
        SQLSelectQueryBlock queryBlock = this.sqlInterpreter.getQueryBlock();
        SQLSelectQueryBlock oldQueryBlock = new SQLSelectQueryBlock();
        queryBlock.cloneTo(oldQueryBlock);

        SQLSelectQueryBlock newQueryBlock = new SQLSelectQueryBlock();
        String newTableName = this.sqlInterpreter.nextTableAlias();

        Map<String, OperatorField> aliasFieldMapping = this.sqlInterpreter.aliasFieldMapping();
        oldQueryBlock.getSelectList().forEach(f -> {
            SQLPropertyExpr sqlExpr = fullField(newTableName, unQuote(f.getAlias()));
            String alias = this.sqlInterpreter.nextColumnAlias();
            newQueryBlock.addSelectItem(sqlExpr, quote(alias));
            aliasFieldMapping.get(unQuote(f.getAlias())).setSqlExpr(sqlExpr);
            aliasFieldMapping.get(unQuote(f.getAlias())).setAlias(alias);
        });

        newQueryBlock.setFrom(tableWithAlias(oldQueryBlock, newTableName));

        queryBlock.setGroupBy(null);
        queryBlock.setWhere(null);
        newQueryBlock.cloneTo(queryBlock);
    }

    public void checkOperator() {
        if (Objects.isNull(this.operator.getValue())) {
            throw new IllegalArgumentException("value is null.");
        }
    }

    protected boolean silentCheckOperator() {
        try {
            this.checkOperator();
            return true;
        } catch (IllegalArgumentException e) {
            log.info("checkOperator silent IllegalArgumentException -> {}", e.getMessage());
            return false;
        }
    }
}
