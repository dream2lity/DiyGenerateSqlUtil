package org.example.sql.interpreter.operator;

import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import org.example.enums.FieldTypeEnum;
import org.example.enums.FieldTypeTransformEnum;
import org.example.sql.OperatorSqlInterpreter;
import org.example.sql.base.OperatorCheckResult;
import org.example.sql.base.OperatorField;
import org.example.sql.interpreter.OperatorInterpreter;
import org.example.sql.operator.SetFieldOperator;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.example.sql.SqlBuildHelper.*;

/**
 * @author chijiuwang
 */
public class SetFieldOperatorInterpreter extends OperatorInterpreter<SetFieldOperator> {
    public SetFieldOperatorInterpreter(SetFieldOperator operator, OperatorSqlInterpreter sqlInterpreter) {
        super(operator, sqlInterpreter);
    }

    @Override
    public void checkOperators() {
        OperatorCheckResult checkResult = new OperatorCheckResult();
        this.sqlInterpreter.getOperatorCheckResults().add(checkResult);

        if (!silentCheckOperator()) {
            return;
        }

        List<SetFieldOperator.FieldSetters> fieldSetters = this.operator.getValue().getFieldList();
        List<String> preFieldIds = this.sqlInterpreter.getCheckFields().stream().map(OperatorField::getFieldId).collect(Collectors.toList());
        this.sqlInterpreter.clearCheckFields();
        fieldSetters.forEach(s -> {
            if (s.getUsed()) {
                if (!preFieldIds.contains(s.getFieldId())) {
                    OperatorCheckResult.MissFieldInfo missFieldInfo = new OperatorCheckResult.MissFieldInfo();
                    missFieldInfo.setFieldId(s.getFieldId());
                    checkResult.addMissFieldInfo(missFieldInfo);
                } else {
                    this.sqlInterpreter.addCheckField(convertFieldType(s.getFieldType()), s.getTransferName(), null);
                }
            }
        });


    }

    @Override
    public void generateFieldList() {
        List<SetFieldOperator.FieldSetters> fieldSetters = this.operator.getValue().getFieldList();
        List<OperatorField> preFields = new ArrayList<>(this.sqlInterpreter.getFields());
        this.sqlInterpreter.clearFields();
        fieldSetters.forEach(f -> {
            if (f.getUsed()) {
                this.sqlInterpreter.addField(convertFieldType(f.getFieldType()), f.getTransferName(), findSourceField(f.getFieldId(), preFields));
            }
        });
    }

    @Override
    public void generateSql() {
        List<SetFieldOperator.FieldSetters> setFieldValue = this.operator.getValue().getFieldList();

        SQLSelectQueryBlock queryBlock = this.sqlInterpreter.getQueryBlock();
        List<OperatorField> preFields = new ArrayList<>(this.sqlInterpreter.getFields());

        fillTypeSpecified(preFields, setFieldValue);

        List<SetFieldOperator.FieldSetters> changedFields = setFieldValue.stream().filter(f -> f.getTypeSpecified() || !f.getUsed()).collect(Collectors.toList());
        Map<SetFieldOperator.FieldSetters , OperatorField> preFieldMapping = setFieldValue.stream()
                .collect(Collectors.toMap(f -> f, f -> findSourceField(f.getFieldId(), preFields), (o1, o2) -> o1));

        if (this.sqlInterpreter.isNeedSubQuery()) {
            generateSubQuery();
        }

        Map<String, FieldTypeEnum> fieldIdTypeMapping = preFields.stream().collect(Collectors.toMap(OperatorField::getFieldId, OperatorField::getFieldType));

        List<SQLSelectItem> selectList = queryBlock.getSelectList();
        int specifiedTypeFieldCount = 0;
        for (int i = 0; i < selectList.size(); i++) {
            SQLSelectItem selectItem = selectList.get(i);
            for (SetFieldOperator.FieldSetters fieldSetters : changedFields) {
                if (selectItem.getAlias().equals(quote(preFieldMapping.get(fieldSetters).getAlias()))) {
                    if (!fieldSetters.getUsed()) {
                        selectList.remove(i--);
                    } else if (fieldSetters.getTypeSpecified()) {
                        specifiedTypeFieldCount++;
                        FieldTypeEnum fieldTypeEnum = FieldTypeEnum.convertByCode(fieldSetters.getFieldType());
                        FieldTypeTransformEnum fieldTypeTransformEnum = FieldTypeTransformEnum.convertByToType(fieldIdTypeMapping.get(fieldSetters.getFieldId()).getCode(), fieldTypeEnum.getCode());
                        SQLIdentifierExpr sqlExpr = wrapFieldWithFunction(selectItem.getExpr(), fieldTypeTransformEnum.getFunctionFormat(), fieldTypeTransformEnum.getArgCount());
                        selectItem.setExpr(sqlExpr);
                        preFieldMapping.get(fieldSetters).setSqlExpr(sqlExpr);
                    }
                }
            }
        }

        this.sqlInterpreter.clearFields();
        List<SQLSelectItem> sortedSelectList = new ArrayList<>(selectList.size());
        setFieldValue.forEach(f -> {
            if (f.getUsed()) {
                OperatorField preField = preFieldMapping.get(f);
                SQLSelectItem selectItem = selectList.stream().filter(i -> quote(preField.getAlias()).equals(i.getAlias())).findFirst().orElseThrow(() -> new IllegalArgumentException("set_field sorted field not found."));
                sortedSelectList.add(selectItem);
                this.sqlInterpreter.addField(convertFieldType(f.getFieldType()), f.getTransferName(), selectItem.getExpr(), unQuote(selectItem.getAlias()), preField.getSourceField());
            }
        });
        selectList.clear();
        selectList.addAll(sortedSelectList);

        this.sqlInterpreter.setNeedSubQuery(specifiedTypeFieldCount > 0 || this.sqlInterpreter.isNeedSubQuery());
    }

    @Override
    public void checkOperator() {
        super.checkOperator();
        SetFieldOperator.FieldList value = this.operator.getValue();
        if (CollectionUtils.isEmpty(value.getFieldList()) || value.getFieldList().stream().noneMatch(SetFieldOperator.FieldSetters::getUsed)) {
            throw new IllegalArgumentException("字段设置中至少选择一个字段");
        }
    }

    private void fillTypeSpecified(List<OperatorField> preFields, List<SetFieldOperator.FieldSetters> setFieldValue) {
        Map<String, FieldTypeEnum> fieldIdTypeMapping = preFields.stream().collect(Collectors.toMap(OperatorField::getFieldId, OperatorField::getFieldType));
        for (SetFieldOperator.FieldSetters fieldSetters : setFieldValue) {
            fieldSetters.setTypeSpecified(fieldSetters.getFieldType() != fieldIdTypeMapping.get(fieldSetters.getFieldId()).getCode());
        }
    }

}
