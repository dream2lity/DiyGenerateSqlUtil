package org.example.sql.interpreter.operator;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import org.example.enums.*;
import org.example.sql.OperatorSqlInterpreter;
import org.example.sql.base.OperatorCheckResult;
import org.example.sql.base.OperatorField;
import org.example.sql.interpreter.OperatorInterpreter;
import org.example.sql.operator.GroupByOperator;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.example.sql.SqlBuildHelper.*;

/**
 * @author chijiuwang
 */
public class GroupByOperatorInterpreter extends OperatorInterpreter<GroupByOperator> {
    public GroupByOperatorInterpreter(GroupByOperator operator, OperatorSqlInterpreter sqlInterpreter) {
        super(operator, sqlInterpreter);
    }

    @Override
    public void checkOperators() {
        OperatorCheckResult checkResult = new OperatorCheckResult();
        this.sqlInterpreter.getOperatorCheckResults().add(checkResult);

        if (!silentCheckOperator()) {
            return;
        }

        List<String> preFieldIds = this.sqlInterpreter.getCheckFields().stream().map(OperatorField::getFieldId).collect(Collectors.toList());
        List<GroupByOperator.GroupByFieldInfo> fields = CollectionUtils.isEmpty(this.operator.getValue().getDimensions()) ? new ArrayList<>() : new ArrayList<>(this.operator.getValue().getDimensions());
        List<GroupByOperator.GroupByFieldInfo> norms = CollectionUtils.isEmpty(this.operator.getValue().getNorms()) ? new ArrayList<>() : new ArrayList<>(this.operator.getValue().getNorms());
        norms.forEach(n -> n.setFieldType(FieldTypeEnum.NUMBER.getCode()));
        fields.addAll(norms);
        this.sqlInterpreter.clearCheckFields();
        fields.forEach(d -> {
            if (!preFieldIds.contains(d.getFieldId())) {
                OperatorCheckResult.MissFieldInfo missFieldInfo = new OperatorCheckResult.MissFieldInfo();
                missFieldInfo.setFieldId(d.getFieldId());
                checkResult.addMissFieldInfo(missFieldInfo);
            } else {
                this.sqlInterpreter.addCheckField(convertFieldType(d.getFieldType()), d.getTransferName(), null);
            }
        });

    }

    @Override
    public void generateFieldList() {
        List<OperatorField> preFields = new ArrayList<>(this.sqlInterpreter.getFields());
        this.sqlInterpreter.clearFields();
        if (!CollectionUtils.isEmpty(this.operator.getValue().getDimensions())) {
            this.operator.getValue().getDimensions().forEach(d ->
                    this.sqlInterpreter.addField(convertFieldType(d.getFieldType()), d.getTransferName(), findSourceField(d.getFieldId(), preFields))
            );
        }
        if (!CollectionUtils.isEmpty(this.operator.getValue().getNorms())) {
            this.operator.getValue().getNorms().forEach(n ->
                    this.sqlInterpreter.addField(FieldTypeEnum.NUMBER, n.getTransferName(), findSourceField(n.getFieldId(), preFields))
            );
        }
    }

    @Override
    public void generateSql() {
        SQLSelectQueryBlock queryBlock = this.sqlInterpreter.getQueryBlock();
        SQLSelectQueryBlock oldQueryBlock = new SQLSelectQueryBlock();
        queryBlock.cloneTo(oldQueryBlock);

        SQLSelectQueryBlock newQueryBlock = new SQLSelectQueryBlock();
        String newTableName = this.sqlInterpreter.nextTableAlias();

        GroupByOperator.GroupInfo groupByValue = this.operator.getValue();
        List<GroupByOperator.GroupByFieldInfo> dimensions = groupByValue.getDimensions();
        List<GroupByOperator.GroupByFieldInfo> norms = groupByValue.getNorms();

        List<OperatorField> preFields = new ArrayList<>(this.sqlInterpreter.getFields());
        Map<String, OperatorField> preFieldMapping = preFields.stream().collect(Collectors.toMap(OperatorField::getFieldId, f -> f, (o1, o2) -> o1));

        this.sqlInterpreter.clearFields();
        if (!CollectionUtils.isEmpty(dimensions)) {
            dimensions.forEach(d -> {
                String alias = this.sqlInterpreter.nextColumnAlias();
                OperatorField preField = preFieldMapping.get(d.getFieldId());
                if (d.getFieldType() == FieldTypeEnum.DATE.getCode() || d.getFieldType() == FieldTypeEnum.DATETIME.getCode()) {
                    // 设置默认值
                    DateFunctionEnum functionEnum = DateFunctionEnum.convertByCode(Objects.isNull(d.getFunctionType()) ? 1 : d.getFunctionType());

                    SQLIdentifierExpr sqlExpr = simpleFunctionField(newTableName, preField.getAlias(),
                            functionEnum.getFunctionFormat(), functionEnum.getArgCount());
                    newQueryBlock.addSelectItem(sqlExpr, quote(alias));
                    this.sqlInterpreter.addField(preField.getFieldType(), d.getTransferName(), sqlExpr, alias, preField);
                } else {
                    SQLPropertyExpr sqlExpr = fullField(newTableName, preField.getAlias());
                    newQueryBlock.addSelectItem(sqlExpr, quote(alias));
                    this.sqlInterpreter.addField(preField.getFieldType(), d.getTransferName(), sqlExpr, alias, preField);
                }
            });
        }
        if (!CollectionUtils.isEmpty(norms)) {
            norms.forEach(n -> {
                String alias = this.sqlInterpreter.nextColumnAlias();
                OperatorField preField = preFieldMapping.get(n.getFieldId());
                SQLIdentifierExpr sqlExpr;
                if (n.getFieldType() == FieldTypeEnum.DATE.getCode() || n.getFieldType() == FieldTypeEnum.DATETIME.getCode()) {
                    DateAggFuncEnum funcEnum = DateAggFuncEnum.convertByCode(n.getFunctionType());
                    sqlExpr = simpleFunctionField(newTableName, preField.getAlias(),
                            funcEnum.getFunctionFormat(), funcEnum.getArgCount());
                    newQueryBlock.addSelectItem(sqlExpr, quote(alias));
                } else if (n.getFieldType() == FieldTypeEnum.STRING.getCode()) {
                    TextAggFuncEnum funcEnum = TextAggFuncEnum.convertByCode(n.getFunctionType());
                    sqlExpr = simpleFunctionField(newTableName, preField.getAlias(),
                            funcEnum.getFunctionFormat(), funcEnum.getArgCount());
                    newQueryBlock.addSelectItem(sqlExpr, quote(alias));
                } else if (n.getFieldType() == FieldTypeEnum.NUMBER.getCode()) {
                    NumberAggFuncEnum funcEnum = NumberAggFuncEnum.convertByCode(n.getFunctionType());
                    sqlExpr = simpleFunctionField(newTableName, preField.getAlias(),
                            funcEnum.getFunctionFormat(), funcEnum.getArgCount());
                    newQueryBlock.addSelectItem(sqlExpr, quote(alias));
                } else {
                    throw new IllegalArgumentException("不支持的字段类型");
                }
                this.sqlInterpreter.addField(FieldTypeEnum.NUMBER, n.getTransferName(), sqlExpr, alias, preField.getSourceField());
            });
        }

        newQueryBlock.setFrom(tableWithAlias(oldQueryBlock, newTableName));

        newQueryBlock.setGroupBy(generateGroupBy(dimensions.size()));

        queryBlock.setWhere(null);
        newQueryBlock.cloneTo(queryBlock);

        this.sqlInterpreter.setNeedSubQuery(true);
    }

    @Override
    public void checkOperator() {
        super.checkOperator();
        GroupByOperator.GroupInfo value = this.operator.getValue();
        if (CollectionUtils.isEmpty(value.getDimensions()) && CollectionUtils.isEmpty(value.getNorms())) {
            throw new IllegalArgumentException("分组汇总中分组或汇总未选择字段");
        }
    }
}
