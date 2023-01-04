package org.example.sql.interpreter.operator;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.fasterxml.jackson.core.type.TypeReference;
import org.example.enums.FilterTypeEnum;
import org.example.sql.OperatorSqlInterpreter;
import org.example.sql.base.OperatorCheckResult;
import org.example.sql.base.OperatorField;
import org.example.sql.base.PlaceHolderInfo;
import org.example.sql.base.filter.DateRangeDay;
import org.example.sql.base.filter.DateSimpleDay;
import org.example.sql.base.filter.NumberBetween;
import org.example.sql.base.filter.StringBelong;
import org.example.sql.interpreter.OperatorInterpreter;
import org.example.sql.operator.FilterOperator;
import org.example.enums.FieldTypeEnum;
import org.example.sql.SqlBuildHelper;
import org.springframework.util.StringUtils;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author chijiuwang
 */
public class FilterOperatorInterpreter extends OperatorInterpreter<FilterOperator> {

    private static final DateTimeFormatter YYYY_MM_DD = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public FilterOperatorInterpreter(FilterOperator operator, OperatorSqlInterpreter sqlInterpreter) {
        super(operator, sqlInterpreter);
    }

    @Override
    public void checkOperators() {
        OperatorCheckResult checkResult = new OperatorCheckResult();
        this.sqlInterpreter.getOperatorCheckResults().add(checkResult);

        FilterOperator.Filters<Object> filterValue = this.operator.getValue();

        Integer[] conditionFilterTypes = FilterTypeEnum.getConditionFilterTypes().stream().map(FilterTypeEnum::getCode).toArray(Integer[]::new);
        this.operator.transferFilterValue(filterValue, conditionFilterTypes, this.sqlInterpreter.getObjectMapper());

        Map<String, OperatorField> preFieldMaps = this.sqlInterpreter.getCheckFields().stream().collect(Collectors.toMap(OperatorField::getFieldId, f -> f));
        checkFilterValue(filterValue, preFieldMaps, checkResult);

        generateFieldList();
    }

    @Override
    public void generateFieldList() {

    }

    @Override
    public void generateSql() {
        FilterOperator.Filters<Object> filterValue = this.operator.getValue();

        Integer[] conditionFilterTypes = FilterTypeEnum.getConditionFilterTypes().stream().map(FilterTypeEnum::getCode).toArray(Integer[]::new);
        this.operator.transferFilterValue(filterValue, conditionFilterTypes, this.sqlInterpreter.getObjectMapper());

        SQLSelectQueryBlock queryBlock = this.sqlInterpreter.getQueryBlock();
        if (this.sqlInterpreter.isNeedSubQuery()) {
            generateSubQuery();
        }
        queryBlock.addWhere(new SQLIdentifierExpr("1 = 1"));
        queryBlock.addWhere(generateWhere(filterValue));

        this.sqlInterpreter.setNeedSubQuery(true);
    }

    private void checkFilterValue(FilterOperator.Filters<Object> filter, Map<String, OperatorField> preFieldMaps, OperatorCheckResult checkResult) {
        if (Objects.isNull(filter.getFilterType())) {
            return;
        }
        FilterTypeEnum filterTypeEnum = FilterTypeEnum.convertByCode(filter.getFilterType());
        switch (filterTypeEnum) {
            case AND:
            case OR:
                List<FilterOperator.Filters<Object>> filters = this.sqlInterpreter.getObjectMapper().convertValue(filter.getFilterValue(), new TypeReference<List<FilterOperator.Filters<Object>>>() {
                });
                filters.forEach(f -> checkFilterValue(f, preFieldMaps, checkResult));
                break;
            default:
                if (StringUtils.hasText(filter.getFieldId())) {
                    if (!preFieldMaps.containsKey(filter.getFieldId())) {
                        OperatorCheckResult.MissFieldInfo missFieldInfo = new OperatorCheckResult.MissFieldInfo();
                        missFieldInfo.setFieldId(filter.getFieldId());
                        checkResult.addMissFieldInfo(missFieldInfo);
                    } else {
                        OperatorField operatorField = preFieldMaps.get(filter.getFieldId());
                        if (filterTypeEnum == FilterTypeEnum.PLACEHOLDER) {
                            break;
                        }
                        switch (operatorField.getFieldType()) {
                            case STRING:
                                if (!FilterTypeEnum.getTextFilterTypes().contains(filterTypeEnum)) {
                                    OperatorCheckResult.FieldTypeNotMatchInfo fieldTypeNotMatchInfo = new OperatorCheckResult.FieldTypeNotMatchInfo();
                                    fieldTypeNotMatchInfo.setFieldId(filter.getFieldId());
                                    checkResult.addFieldTypeNotMatchInfo(fieldTypeNotMatchInfo);
                                }
                                break;
                            case NUMBER:
                                if (!FilterTypeEnum.getNumberFilterTypes().contains(filterTypeEnum)) {
                                    OperatorCheckResult.FieldTypeNotMatchInfo fieldTypeNotMatchInfo = new OperatorCheckResult.FieldTypeNotMatchInfo();
                                    fieldTypeNotMatchInfo.setFieldId(filter.getFieldId());
                                    checkResult.addFieldTypeNotMatchInfo(fieldTypeNotMatchInfo);
                                }
                                break;
                            case DATE:
                            case DATETIME:
                                if (!FilterTypeEnum.getDateFilterTypes().contains(filterTypeEnum)) {
                                    OperatorCheckResult.FieldTypeNotMatchInfo fieldTypeNotMatchInfo = new OperatorCheckResult.FieldTypeNotMatchInfo();
                                    fieldTypeNotMatchInfo.setFieldId(filter.getFieldId());
                                    checkResult.addFieldTypeNotMatchInfo(fieldTypeNotMatchInfo);
                                }
                                break;
                            default:
                        }
                    }
                }
        }
    }

    private SQLExpr generateWhere(FilterOperator.Filters<Object> filter) {
        if (Objects.isNull(filter.getFilterType())) {
            return null;
        }
        FilterTypeEnum filterTypeEnum = FilterTypeEnum.convertByCode(filter.getFilterType());
        String fieldId = filter.getFieldId();
        if (!FilterTypeEnum.getConditionFilterTypes().contains(filterTypeEnum) && !StringUtils.hasText(fieldId)) {
            return null;
        }
        SQLExpr fieldExpr = null;
        OperatorField fieldInfo = null;
        if (StringUtils.hasText(fieldId)) {
            // value 没有值
            if (!FilterTypeEnum.getVoidValueTypes().contains(filterTypeEnum) && filter.getFilterValue() instanceof String && StringUtils.isEmpty(filter.getFilterValue())) {
                return null;
            }
            fieldInfo = findSourceField(fieldId, this.sqlInterpreter.getFields());
            fieldExpr = fieldInfo.getSqlExpr();
        } else if (!FilterTypeEnum.getConditionFilterTypes().contains(filterTypeEnum)) {
            throw new IllegalArgumentException("fieldId could not be null.");
        }

        switch (filterTypeEnum) {
            case AND:
            case OR:
                List<FilterOperator.Filters<Object>> filters = this.sqlInterpreter.getObjectMapper().convertValue(filter.getFilterValue(), new TypeReference<List<FilterOperator.Filters<Object>>>() {
                });
                List<SQLExpr> filterExprList = filters.stream().map(this::generateWhere).collect(Collectors.toList());
                return SqlBuildHelper.groupMultiCondition(filterTypeEnum.getOps()[0], filterExprList);
            case BETWEEN:
            case NOT_BETWEEN:
                NumberBetween numberBetween = this.sqlInterpreter.getObjectMapper().convertValue(filter.getFilterValue(), NumberBetween.class);
                if (Objects.isNull(numberBetween.getMin()) && !Objects.isNull(numberBetween.getMax())) {
                    return new SQLIdentifierExpr("1 = 1");
                }
                SQLExpr numberBetweenExpr = null;
                if (Objects.nonNull(numberBetween.getMin())) {
                    numberBetweenExpr = new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[0], new SQLIdentifierExpr(String.valueOf(numberBetween.getMin())));
                }
                if (Objects.nonNull(numberBetween.getMax())) {
                    SQLBinaryOpExpr numberBetweenMaxExpr = new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[1], new SQLIdentifierExpr(String.valueOf(numberBetween.getMax())));
                    numberBetweenExpr = Objects.isNull(numberBetweenExpr) ?
                            numberBetweenMaxExpr : SqlBuildHelper.groupMultiCondition(filterTypeEnum.getOps()[2], Arrays.asList(numberBetweenExpr, numberBetweenMaxExpr));
                }
                return numberBetweenExpr;
            case EQUALS:
            case NOT_EQUALS:
            case GREAT_THAN:
            case LESS_THAN:
            case GREAT_THAN_EQUALS:
            case LESS_THAN_EQUALS:
                String val = String.valueOf(filter.getFilterValue());
                if (!StringUtils.hasText(val)) {
                    return null;
                }
                return new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[0], new SQLIdentifierExpr(val));
            case IS_NULL:
            case NOT_NULL:
            case DATE_IS_NULL:
            case DATE_NOT_NULL:
                return new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[0], new SQLNullExpr());
            case BELONG:
                ArrayList<StringBelong> filterInValue = this.sqlInterpreter.getObjectMapper().convertValue(filter.getFilterValue(), new TypeReference<ArrayList<StringBelong>>() {
                });
                SQLInListExpr sqlInListExpr = new SQLInListExpr(fieldExpr);
                filterInValue.forEach(v -> sqlInListExpr.addTarget(new SQLCharExpr(v.getValue())));
                return sqlInListExpr;
            case NOT_BELONG:
                ArrayList<StringBelong> filterNotInValue = this.sqlInterpreter.getObjectMapper().convertValue(filter.getFilterValue(), new TypeReference<ArrayList<StringBelong>>() {
                });
                SQLInListExpr sqlNotInListExpr = new SQLInListExpr(fieldExpr, true);
                filterNotInValue.forEach(v -> sqlNotInListExpr.addTarget(new SQLCharExpr(v.getValue())));
                return sqlNotInListExpr;
            case CONTAIN:
            case NOT_CONTAIN:
                return new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[0], new SQLCharExpr("%" + filter.getFilterValue() + "%"));
            case TEXT_IS_NULL:
            case TEXT_NOT_NULL:
                return SqlBuildHelper.groupMultiCondition(
                        filterTypeEnum.getOps()[1],
                        Arrays.asList(
                                new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[0], new SQLCharExpr("")),
                                new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[2], new SQLNullExpr())
                        )
                );
            case START_WITH:
            case START_NOT_WITH:
                return new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[0], new SQLCharExpr(filter.getFilterValue() + "%"));
            case END_WITH:
            case END_NOT_WITH:
                return new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[0], new SQLCharExpr("%" + filter.getFilterValue()));
            case DATE_BELONG:
            case DATE_NOT_BELONG:
                DateRangeDay dateRangeDay = this.sqlInterpreter.getObjectMapper().convertValue(filter.getFilterValue(), DateRangeDay.class);
                LocalDate startTime = LocalDate.parse(dateRangeDay.getStartTime(), YYYY_MM_DD);
                LocalDate endTime = LocalDate.parse(dateRangeDay.getEndTime(), YYYY_MM_DD);
                LocalDate endTimePlus = endTime.plusDays(1);
                return SqlBuildHelper.groupMultiCondition(
                        filterTypeEnum.getOps()[2],
                        Arrays.asList(
                                new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[0], new SQLTimestampExpr(Date.from(startTime.atStartOfDay(ZoneId.systemDefault()).toInstant()))),
                                new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[1], new SQLTimestampExpr(Date.from(endTimePlus.atStartOfDay(ZoneId.systemDefault()).toInstant())))
                        )
                );
            case DATE_EQUALS:
            case DATE_NOT_EQUALS:
                DateSimpleDay dateSimpleDay = this.sqlInterpreter.getObjectMapper().convertValue(filter.getFilterValue(), DateSimpleDay.class);
                LocalDate date = LocalDate.parse(dateSimpleDay.getDate(), YYYY_MM_DD);
                LocalDate datePlus = date.plusDays(1);
                return SqlBuildHelper.groupMultiCondition(
                        filterTypeEnum.getOps()[2],
                        Arrays.asList(
                                new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[0], new SQLTimestampExpr(Date.from(date.atStartOfDay(ZoneId.systemDefault()).toInstant()))),
                                new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[1], new SQLTimestampExpr(Date.from(datePlus.atStartOfDay(ZoneId.systemDefault()).toInstant())))
                        )
                );
            case DATE_BEFORE:
                DateSimpleDay beforeDateSimpleDay = this.sqlInterpreter.getObjectMapper().convertValue(filter.getFilterValue(), DateSimpleDay.class);
                LocalDate beforeDate = LocalDate.parse(beforeDateSimpleDay.getDate(), YYYY_MM_DD);
                return new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[0], new SQLTimestampExpr(Date.from(beforeDate.plusDays(1).atStartOfDay(ZoneId.systemDefault()).toInstant())));
            case DATE_AFTER:
                DateSimpleDay afterDateSimpleDay = this.sqlInterpreter.getObjectMapper().convertValue(filter.getFilterValue(), DateSimpleDay.class);
                LocalDate afterDate = LocalDate.parse(afterDateSimpleDay.getDate(), YYYY_MM_DD);
                return new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[0], new SQLTimestampExpr(Date.from(afterDate.atStartOfDay(ZoneId.systemDefault()).toInstant())));
            case PLACEHOLDER:
                String placeHolderName = (String) filter.getFilterValue();
                this.sqlInterpreter.getPlaceHolderInfos().add(
                        new PlaceHolderInfo(placeHolderName, SQLUtils.toSQLString(Objects.requireNonNull(fieldExpr)), fieldInfo.getFieldType(), fieldId)
                );
                return this.sqlInterpreter.isBuildWithPlaceHolder() ? new SQLIdentifierExpr(SqlBuildHelper.generatePlaceholder(SQLUtils.toSQLString(fieldExpr))) : null;
            default:
                throw new IllegalArgumentException("unsupported operator.");
        }
    }
}
