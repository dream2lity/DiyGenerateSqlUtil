package org.example.enums;

import com.alibaba.druid.sql.ast.expr.SQLBinaryOperator;
import org.example.sql.base.filter.DateRangeDay;
import org.example.sql.base.filter.DateSimpleDay;
import org.example.sql.base.filter.NumberBetween;
import org.example.sql.operator.FilterOperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.druid.sql.ast.expr.SQLBinaryOperator.*;

/**
 * 扩展方式：<br/>
 * <li>增加这个枚举的完整实例</li>
 * <li>在{@link FilterOperator}的generateWhere方法的switch中增加对应的处理流程</li>
 * <br/>
 * 说明：<br/>
 * 这里的属性ops取自{@link SQLBinaryOperator}
 *
 * @author chijiuwang
 */
public enum FilterTypeEnum {
    /**
     *  过滤条件关联关系
     */
    AND(1, "且", FilterOperator.Filters.class, true, new SQLBinaryOperator[]{BooleanAnd}),
    OR(2, "或", FilterOperator.Filters.class, true, new SQLBinaryOperator[]{BooleanOr}),

    /**
     * FieldTypeEnum.NUMBER
     * @see FieldTypeEnum
     */
    BETWEEN(3, "区间", NumberBetween.class, false, new SQLBinaryOperator[]{GreaterThanOrEqual, LessThanOrEqual, BooleanAnd}),
    NOT_BETWEEN(4, "排除区间", NumberBetween.class, false, new SQLBinaryOperator[]{LessThanOrEqual, GreaterThanOrEqual, BooleanOr}),
    EQUALS(5, "等于", Double.class, false, new SQLBinaryOperator[]{Equality}),
    NOT_EQUALS(6, "不等于", Double.class, false, new SQLBinaryOperator[]{NotEqual}),
    GREAT_THAN(7, "大于", Double.class, false, new SQLBinaryOperator[]{GreaterThan}),
    LESS_THAN(8, "小于", Double.class, false, new SQLBinaryOperator[]{LessThan}),
    GREAT_THAN_EQUALS(9, "大于等于", Double.class, false, new SQLBinaryOperator[]{GreaterThanOrEqual}),
    LESS_THAN_EQUALS(10, "小于等于", Double.class, false, new SQLBinaryOperator[]{LessThanOrEqual}),
    IS_NULL(11, "为空", Void.class, false, new SQLBinaryOperator[]{Is}),
    NOT_NULL(12, "非空", Void.class, false, new SQLBinaryOperator[]{IsNot}),

    /**
     * FieldTypeEnum.TEXT
     * @see FieldTypeEnum
     */
    BELONG(13, "属于", ArrayList.class, true, new SQLBinaryOperator[]{}),
    NOT_BELONG(14, "不属于", ArrayList.class, true, new SQLBinaryOperator[]{}),
    CONTAIN(15, "包含", String.class, false, new SQLBinaryOperator[]{Like}),
    NOT_CONTAIN(16, "不包含", String.class, false, new SQLBinaryOperator[]{NotLike}),
    TEXT_IS_NULL(17, "为空", Void.class, false, new SQLBinaryOperator[]{Equality, BooleanAnd, Is}),
    TEXT_NOT_NULL(18, "非空", Void.class, false, new SQLBinaryOperator[]{NotEqual, BooleanAnd, IsNot}),
    START_WITH(19, "开头是", String.class, false, new SQLBinaryOperator[]{Like}),
    START_NOT_WITH(20, "开头不是", String.class, false, new SQLBinaryOperator[]{NotLike}),
    END_WITH(21, "结尾是", String.class, false, new SQLBinaryOperator[]{Like}),
    END_NOT_WITH(22, "结尾不是", String.class, false, new SQLBinaryOperator[]{NotLike}),

    /**
     * FieldTypeEnum.DATE
     * @see FieldTypeEnum
     */
    DATE_BELONG(23, "属于", DateRangeDay.class, false, new SQLBinaryOperator[]{GreaterThanOrEqual, LessThan, BooleanAnd}),
    DATE_NOT_BELONG(24, "不属于", DateRangeDay.class, false, new SQLBinaryOperator[]{LessThan, GreaterThanOrEqual, BooleanOr}),
    DATE_EQUALS(25, "等于", DateSimpleDay.class, false, new SQLBinaryOperator[]{GreaterThanOrEqual, LessThan, BooleanAnd}),
    DATE_NOT_EQUALS(26, "不等于", DateSimpleDay.class, false, new SQLBinaryOperator[]{LessThan, GreaterThanOrEqual, BooleanOr}),
    DATE_IS_NULL(27, "为空", Void.class, false, new SQLBinaryOperator[]{Is}),
    DATE_NOT_NULL(28, "非空", Void.class, false, new SQLBinaryOperator[]{IsNot}),
    DATE_BEFORE(29, "某个日期之前", DateSimpleDay.class, false, new SQLBinaryOperator[]{LessThan}),
    DATE_AFTER(30, "某个日期之后", DateSimpleDay.class, false, new SQLBinaryOperator[]{GreaterThanOrEqual}),

    PLACEHOLDER(31, "透传筛选器", String.class, false, new SQLBinaryOperator[0]),
    ;

    int code;
    String desc;
    /**
     * 具体值的类型，参数格式
     */
    Class<?> valueClass;
    /**
     * 是否包含多个参数
     */
    boolean isMultiple;
    /**
     * 使用到的操作符
     */
    SQLBinaryOperator[] ops;

    FilterTypeEnum(int code, String desc, Class<?> valueClass, boolean isMultiple, SQLBinaryOperator[] ops) {
        this.code = code;
        this.desc = desc;
        this.valueClass = valueClass;
        this.isMultiple = isMultiple;
        this.ops = ops;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }

    public Class<?> getValueClass() {
        return valueClass;
    }

    public boolean isMultiple() {
        return isMultiple;
    }

    public SQLBinaryOperator[] getOps() {
        return ops;
    }

    public static List<FilterTypeEnum> getVoidValueTypes() {
        return Arrays.stream(FilterTypeEnum.values()).filter(t -> t.getValueClass() == Void.class).collect(Collectors.toList());
    }

    public static List<FilterTypeEnum> getConditionFilterTypes() {
        return Arrays.asList(AND, OR);
    }

    public static List<FilterTypeEnum> getNumberFilterTypes() {
        return Arrays.asList(
                BETWEEN,
                NOT_BETWEEN,
                EQUALS,
                NOT_EQUALS,
                GREAT_THAN,
                LESS_THAN,
                GREAT_THAN_EQUALS,
                LESS_THAN_EQUALS,
                IS_NULL,
                NOT_NULL
        );
    }

    public static List<FilterTypeEnum> getTextFilterTypes() {
        return Arrays.asList(
                BELONG,
                NOT_BELONG,
                CONTAIN,
                NOT_CONTAIN,
                TEXT_IS_NULL,
                TEXT_NOT_NULL,
                START_WITH,
                START_NOT_WITH,
                END_WITH,
                END_NOT_WITH
        );
    }

    public static List<FilterTypeEnum> getDateFilterTypes() {
        return Arrays.asList(
                DATE_BELONG,
                DATE_NOT_BELONG,
                DATE_EQUALS,
                DATE_NOT_EQUALS,
                DATE_IS_NULL,
                DATE_NOT_NULL,
                DATE_BEFORE,
                DATE_AFTER
        );
    }

    public static FilterTypeEnum convertByCode(int code) {
        for (FilterTypeEnum t : FilterTypeEnum.values()) {
            if (t.getCode() == code) {
                return t;
            }
        }
        throw new IllegalArgumentException("FilterType unknown.");
    }

}
