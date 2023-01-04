package org.example.sql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.util.JdbcConstants;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.example.enums.*;
import org.example.sql.base.Field;
import org.example.sql.base.PlaceHolderInfo;
import org.example.sql.base.RelationTableInfo;
import org.example.sql.base.filter.DateRangeDay;
import org.example.sql.base.filter.DateSimpleDay;
import org.example.sql.base.filter.NumberBetween;
import org.example.sql.base.filter.StringBelong;
import org.example.sql.operator.*;
import org.springframework.util.CollectionUtils;
import org.springframework.util.DigestUtils;
import org.springframework.util.StringUtils;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.example.sql.SqlBuildHelper.*;

/**
 * 功能：<br/>
 * operators 生成 SQL 及 字段列表 <br/>
 * 使用：<br/>
 * <li>获取SQL： new OperatorSqlAdapter -> buildSql()</li>
 * <li>只获取字段列表： new OperatorSqlAdapter -> generateFieldList() -> getCurrFieldList()</li>
 *
 * 推荐使用{@link OperatorSqlProxy}
 * @author chijiuwang
 */
@Deprecated
@Slf4j
public class OperatorSqlAdapter {

    private static final DateTimeFormatter YYYY_MM_DD = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private static final String TABLE_ALIAS_PREFIX = "t";
    private static final String COLUMN_ALIAS_PREFIX = "__col_";

    /**
     * 表别名后缀， 在当前查询中的序号
     * <p>
     * TABLE_ALIAS_PREFIX+tableIndex=表别名
     */
    private int tableIndex;
    /**
     * 字段别名后缀， 在当前查询中的序号
     * <p>
     * COLUMN_ALIAS_PREFIX+columnIndex=字段别名
     */
    private int columnIndex;

    /**
     * 单表允许的最大行数
     */
    private final int tableRowCount;
    /**
     * 是否限制表查询行数，默认设置，根据 tableRowCount 决定
     */
    private final boolean isTableLimit;

    /**
     * 最终SQL可以追加limit参数， 根据 offset 和 rowCount 确定
     * <p>
     *    需要调用 setOffset 单独设置
     * </p>
     */
    private Integer offset;
    private Integer rowCount;

    /**
     * 是否需要字段别名的有序列表
     * <p>
     * 为true时才生成 fieldAliasSortedList
     */
    private boolean needFieldAliasSortedList;
    private List<String> fieldAliasSortedList;

    /**
     * 缓存表名映射，真实表名-内部生成的表名
     */
    private final Map<String, String> tableNameMappings = new HashMap<>();
    /**
     * 缓存自助数据集表名和查询映射，自助数据集表名-查询
     */
    private final Map<Long, SQLSelectQueryBlock> diyTableQueryMappings = new HashMap<>();
    /**
     * 缓存自助数据集表名和字段列表映射，自助数据集表名-字段列表
     */
    private final Map<Long, List<FieldInfo>> diyTableCurrFields = new HashMap<>();
    /**
     * 当前实例中所有字段
     */
    private final Map<String, FieldInfo> allFields = new HashMap<>();
    /**
     * 当前SQL的查询字段
     */
    private final Map<String, FieldInfo> currFields = new HashMap<>();
    private final Map<String, String> secondTableCurrTransferNames = new HashMap<>();
    /**
     * 当前SQL的有序查询字段id列表
     */
    private final List<String> currSortedFieldIds = new ArrayList<>();
    /**
     * 操作列表，第一个必须是 SelectFieldOperator
     * @see AnalysisOperator
     */
    private final List<AnalysisOperator<?>> operators;
    private final ObjectMapper objectMapper;
    /**
     * 获取表operators的方法
     */
    private Function<Long, List<AnalysisOperator<?>>> getTableOperators;
    /**
     * 上一步SQL是否需要转换为子查询的的标识
     * 转换时机：
     *  - 上一步查询中存在 join group where
     *  - 当前为group
     *  - 上一步为字段设置 且 存在修改字段类型
     *
     */
    private boolean needSubQuery;
    private boolean firstJoin;

    private boolean buildWithPlaceHolder;
    private final List<PlaceHolderInfo> placeHolderInfos = new ArrayList<>();
    private final Set<RelationTableInfo> relatedTables = new HashSet<>();


    public OperatorSqlAdapter(List<AnalysisOperator<?>> operators, ObjectMapper objectMapper, Integer tableRowCount) {
        this.needSubQuery = false;
        firstJoin = false;
        this.tableIndex = 0;
        this.columnIndex = 0;
        this.operators = operators;
        this.objectMapper = objectMapper;
        this.tableRowCount = Objects.isNull(tableRowCount) ? 0 : tableRowCount;
        this.isTableLimit = Objects.nonNull(tableRowCount);
        this.needFieldAliasSortedList = false;
        this.buildWithPlaceHolder = false;
    }

    /**
     * 设置最终的分页参数
     * @param offset 偏移量
     * @param rowCount 行数
     */
    public void setOffset(Integer offset, Integer rowCount) {
        this.offset = offset;
        this.rowCount = rowCount;
    }

    /**
     * 设置获取表operators的方法
     * @param getTableOperators 获取表operators的方法
     */
    public void setGetTableOperators(Function<Long, List<AnalysisOperator<?>>> getTableOperators) {
        this.getTableOperators = getTableOperators;
    }

    /**
     * 设置是否需要字段别名的有序列表
     * @param needFieldAliasSortedList 是否需要字段别名的有序列表
     */
    public void setNeedFieldAliasSortedList(boolean needFieldAliasSortedList) {
        this.needFieldAliasSortedList = needFieldAliasSortedList;
    }

    /**
     * 获取字段别名的有序列表
     * @return 字段别名的有序列表
     */
    public List<String> getFieldAliasSortedList() {
        return fieldAliasSortedList;
    }

    /**
     * 获取当前SQL的查询字段列表 <br/>
     * 需要先调用 generateFieldList() 或 buildSql() 生成
     *
     * @return 查询字段列表
     */
    public List<FieldInfo> getCurrFieldList() {
        return this.currSortedFieldIds.stream().map(this.currFields::get).collect(Collectors.toList());
    }

    public void setBuildWithPlaceHolder(boolean buildWithPlaceHolder) {
        this.buildWithPlaceHolder = buildWithPlaceHolder;
    }

    public List<PlaceHolderInfo> getPlaceholderInfos() {
        return placeHolderInfos;
    }

    public Set<RelationTableInfo> getRelatedTables() {
        return relatedTables;
    }

    /**
     * 只生成字段列表 <br/>
     * 生成 currSortedFieldIds 及 currFields
     */
    public void generateFieldList() {
        long startTime = System.currentTimeMillis();
        generateFieldListWithOperators(this.operators);
        log.info("generate field List with time[{}ms]", System.currentTimeMillis() - startTime);
    }

    /**
     * <h3> 核心方法-构建SQL </h3>
     * 包括生成字段列表和SQL
     * <ul/>
     * @return SQL
     */
    public String buildSql() {
        long startTime = System.currentTimeMillis();
        SQLSelectQueryBlock queryBlock = buildSqlWithOperators(this.operators);

        if (Objects.nonNull(this.offset) && Objects.nonNull(this.rowCount)) {
            queryBlock.limit(this.rowCount, this.offset);
        }

        String buildSql = SQLUtils.toSQLString(queryBlock, JdbcConstants.MYSQL);
        log.info("build sql with time[{}ms] -> {}", System.currentTimeMillis() - startTime, buildSql);

        if (needFieldAliasSortedList) {
            this.fieldAliasSortedList = queryBlock.getSelectList().stream().map(i -> unQuote(i.getAlias())).collect(Collectors.toList());
        }

        return buildSql;
    }

    private void generateFieldListWithOperators(List<AnalysisOperator<?>> operators) {
        checkOperators();
        optimizeOperators();
        operators.forEach(op -> {
            if (op instanceof SelectFieldOperator) {
                SelectFieldOperator selectField = (SelectFieldOperator) op;
                selectFieldFillFields(selectField);
            } else if (op instanceof JoinOperator) {
                JoinOperator join = (JoinOperator) op;
                joinFillFields(join);
            } else if (op instanceof GroupByOperator) {
                GroupByOperator groupBy = (GroupByOperator) op;
                groupByFillFields(groupBy);
            } else if (op instanceof SetFieldOperator) {
                SetFieldOperator setField = (SetFieldOperator) op;
                setFieldFillFields(setField);
            }
        });
    }

    private SQLSelectQueryBlock buildSqlWithOperators(List<AnalysisOperator<?>> operators) {
        checkOperators();
        optimizeOperators();
        SQLSelectQueryBlock queryBlock = new SQLSelectQueryBlock();
        if (isTableLimit) {
            queryBlock.limit(tableRowCount, 0);
        }

        operators.forEach(op -> {
            if (op instanceof SelectFieldOperator) {
                SelectFieldOperator selectField = (SelectFieldOperator) op;
                selectFieldFillFields(selectField);
                doSelectField(selectField, queryBlock);
            } else if (op instanceof JoinOperator) {
                JoinOperator join = (JoinOperator) op;
                joinFillFields(join);
                doJoin(join, queryBlock);
            } else if (op instanceof GroupByOperator) {
                GroupByOperator groupBy = (GroupByOperator) op;
                groupByFillFields(groupBy);
                doGroupBy(groupBy, queryBlock);
            } else if (op instanceof SetFieldOperator) {
                SetFieldOperator setField = (SetFieldOperator) op;
                setFieldFillFields(setField);
                doSetField(setField, queryBlock);
            } else if (op instanceof FilterOperator) {
                FilterOperator filter = (FilterOperator) op;
                doFilter(filter, queryBlock);
            }
        });
        return queryBlock;
    }

    /**
     * 校验operators
     */
    private void checkOperators() {
        if (CollectionUtils.isEmpty(this.operators)) {
            throw new IllegalArgumentException("operators is empty.");
        }
        if (this.operators.size() > 0 && !(this.operators.get(0) instanceof SelectFieldOperator)) {
            throw new IllegalArgumentException("first operator must be select_field.");
        }
    }

    /**
     * 优化操作栈顺序
     * - 字段设置可以作为全局过滤，可以减少查询返回数据量
     */
    private void optimizeOperators() {

    }

    private void selectFieldFillFields(SelectFieldOperator selectField) {
        TableTypeEnum tableTypeEnum = TableTypeEnum.convertByCode(selectField.getValue().getTableType());
        String tableName = selectField.getValue().getTableName();
        Long tableId = selectField.getValue().getTableId();
        List<Field> fieldList = selectField.getValue().getFields();
        switch (tableTypeEnum) {
            case DB:
                fieldList.forEach(f -> {
                    FieldInfo fieldInfo = addField(f.getFieldName(), FieldTypeEnum.convertByCode(f.getFieldType()), fullField(getTableName(tableName), f.getFieldName()));
                    this.currFields.put(fieldInfo.fieldId, fieldInfo);
                    this.currSortedFieldIds.add(fieldInfo.fieldId);
                });
                break;
            case DIY:
                setDiyTableQuery(tableId);
                Map<String, FieldInfo> fieldInfoMap = getCurrFieldList().stream().collect(Collectors.toMap(FieldInfo::getRealTransferName, f -> f, (f1, f2) -> f1));
                this.currFields.clear();
                this.currSortedFieldIds.clear();
                fieldList.forEach(f -> {
                    FieldInfo fieldInfo = fieldInfoMap.get(f.getFieldName());
                    this.currFields.put(fieldInfo.fieldId, fieldInfo);
                    this.currSortedFieldIds.add(fieldInfo.fieldId);
                });
                break;
            default:
                throw new IllegalArgumentException("unsupported table type.");
        }

    }

    private void doSelectField(SelectFieldOperator selectField, SQLSelectQueryBlock queryBlock) {
        SelectFieldOperator.Fields selectFieldValue = selectField.getValue();
        TableTypeEnum tableTypeEnum = TableTypeEnum.convertByCode(selectFieldValue.getTableType());
        switch (tableTypeEnum) {
            case DB:
                String tableName = getTableName(selectFieldValue.getTableName());
                queryBlock.setFrom(tableNameWithAlias(selectFieldValue.getTableName(), tableName));
                List<Field> fields = selectFieldValue.getFields();
                fields.forEach(f -> queryBlock.addSelectItem(fullField(tableName, f.getFieldName()), quote(nextColumnAlias())));

                fillPermissionPlaceHolder(queryBlock, selectFieldValue, tableName);

                break;
            case DIY:
                String newTableName = nextTableAlias();
                SQLSelectQueryBlock diyTableQuery = setDiyTableQuery(selectFieldValue.getTableId());
                queryBlock.setFrom(diyTableQuery, quote(newTableName));
                getCurrFieldList().forEach(f -> queryBlock.addSelectItem(
                        fullField(newTableName, unQuote(Objects.requireNonNull(getCurrentColumnAliasWithFuncPrefix(diyTableQuery, f, this.allFields)))),
                        quote(nextColumnAlias())
                ));
                this.needSubQuery = true;
                break;
            default:
                throw new IllegalArgumentException("unsupported table type.");
        }

    }

    private void joinFillFields(JoinOperator join) {
        TableTypeEnum tableTypeEnum = TableTypeEnum.convertByCode(join.getValue().getTable().getTableType());
        String tableName = join.getValue().getTable().getTableName();
        Long tableId = join.getValue().getTable().getTableId();
        List<Field> fieldList = join.getValue().getTable().getFields();
        this.secondTableCurrTransferNames.clear();
        switch (tableTypeEnum) {
            case DB:
                fieldList.forEach(f -> {
                    FieldInfo fieldInfo = addField(f.getFieldName(), FieldTypeEnum.convertByCode(f.getFieldType()), fullField(getTableName(tableName), f.getFieldName()));
                    this.currFields.put(fieldInfo.fieldId, fieldInfo);
                    this.currSortedFieldIds.add(fieldInfo.fieldId);
                    this.secondTableCurrTransferNames.put(f.getFieldName(), fieldInfo.getRealTransferName());
                });
                break;
            case DIY:
                Map<String, FieldInfo> oldCurrFields = new HashMap<>(this.currFields);
                List<String> oldCurrSortedFieldIds = new ArrayList<>(this.currSortedFieldIds);
                this.currFields.clear();
                this.currSortedFieldIds.clear();
                setDiyTableQuery(tableId);
                Map<String, FieldInfo> fieldInfoMap = getCurrFieldList().stream().collect(Collectors.toMap(FieldInfo::getRealTransferName, f -> f, (f1, f2) -> f1));
                this.currFields.clear();
                this.currSortedFieldIds.clear();
                this.currFields.putAll(oldCurrFields);
                this.currSortedFieldIds.addAll(oldCurrSortedFieldIds);
                fieldList.forEach(f -> {
                    FieldInfo fieldInfo = fieldInfoMap.get(f.getFieldName());
                    fieldInfo.repeatCount = getTransferNameRepeatCount(fieldInfo.transferName);
                    this.currFields.put(fieldInfo.fieldId, fieldInfo);
                    this.currSortedFieldIds.add(fieldInfo.fieldId);
                    this.secondTableCurrTransferNames.put(f.getFieldName(), fieldInfo.getRealTransferName());
                });
                break;
            default:
                throw new IllegalArgumentException("unsupported table type.");
        }

    }

    private void doJoin(JoinOperator join, SQLSelectQueryBlock queryBlock) {
        JoinOperator.JoinInfo joinValue = join.getValue();

        SelectFieldOperator.Fields joinTable = joinValue.getTable();

        TableTypeEnum tableTypeEnum = TableTypeEnum.convertByCode(joinTable.getTableType());
        switch (tableTypeEnum) {
            case DB:
                joinDb(queryBlock, joinValue);
                break;
            case DIY:
                joinDiy(queryBlock, joinValue);
                break;
            default:
                throw new IllegalArgumentException("unsupported table type.");
        }


        this.needSubQuery = true;
    }

    private void subQueryJoin(SQLSelectQueryBlock queryBlock, JoinOperator.JoinInfo joinValue,
                              Function<String, SQLPropertyExpr> joinTableFieldFunction,
                              Supplier<? extends SQLTableSource> joinRightTableSupplier, String placeholderPermissionTableName) {
        JoinTypeEnum joinTypeEnum = JoinTypeEnum.convertByCode(joinValue.getStyle());

        SelectFieldOperator.Fields joinTable = joinValue.getTable();
        SQLSelectQueryBlock oldQueryBlock = new SQLSelectQueryBlock();
        queryBlock.cloneTo(oldQueryBlock);

        SQLSelectQueryBlock newQueryBlock = new SQLSelectQueryBlock();
        String newTableName = nextTableAlias();

        List<List<String>> basis = joinValue.getBasis();
        Map<String, FieldInfo> relationCols = basis.stream().collect(Collectors.toMap(c -> c.get(0), c -> this.currFields.get(c.get(0)), (o1, o2) -> o1));

        Map<FieldInfo, String> currentColumnAlias = getCurrentColumnAliasWithFuncPrefix(oldQueryBlock, new HashSet<>(relationCols.values()), this.allFields);

        oldQueryBlock.getSelectList().forEach(f -> newQueryBlock.addSelectItem(fullField(newTableName, unQuote(f.getAlias())), quote(nextColumnAlias())));
        joinTable.getFields().forEach(f -> newQueryBlock.addSelectItem(joinTableFieldFunction.apply(
                placeholderPermissionTableName == null ? this.secondTableCurrTransferNames.get(f.getFieldName()) : f.getFieldName()
        ), quote(nextColumnAlias())));

        List<SQLBinaryOpExpr> joinConditions = basis.stream().map(c ->
                joinCondition(
                        fullField(newTableName, unQuote(currentColumnAlias.get(relationCols.get(c.get(0))))),
                        joinTableFieldFunction.apply(
                                placeholderPermissionTableName == null ? this.secondTableCurrTransferNames.get(c.get(1)) : c.get(1)
                        ))
        ).collect(Collectors.toList());

        SQLJoinTableSource joinTableSource = new SQLJoinTableSource();
        joinTableSource.setJoinType(SQLJoinTableSource.JoinType.valueOf(joinTypeEnum.name()));
        joinTableSource.setLeft(tableWithAlias(oldQueryBlock, newTableName));
        joinTableSource.setRight(joinRightTableSupplier.get());

        joinTableSource.setCondition(multiJoinCondition(joinConditions));

        newQueryBlock.setFrom(joinTableSource);

        if (placeholderPermissionTableName != null) {
            fillPermissionPlaceHolder(newQueryBlock, joinTable, placeholderPermissionTableName);
        }

        queryBlock.setWhere(null);
        queryBlock.setGroupBy(null);
        newQueryBlock.cloneTo(queryBlock);

        this.firstJoin = false;
    }

    private void joinDiy(SQLSelectQueryBlock queryBlock, JoinOperator.JoinInfo joinValue) {
        JoinTypeEnum joinTypeEnum = JoinTypeEnum.convertByCode(joinValue.getStyle());

        SelectFieldOperator.Fields joinTable = joinValue.getTable();
        if (needSubQuery) {
            String diyTableNewTableName = nextTableAlias();
            SQLSelectQueryBlock diyTableQuery = setDiyTableQuery(joinTable.getTableId());
            Map<String, FieldInfo> fieldInfoMap = this.diyTableCurrFields.get(joinTable.getTableId()).stream().collect(Collectors.toMap(
                    FieldInfo::getRealTransferName,
                    f -> f, (f1, f2) -> f1));

            subQueryJoin(
                    queryBlock,
                    joinValue,
                    fieldName -> fullField(diyTableNewTableName, unQuote(getCurrentColumnAliasWithFuncPrefix(diyTableQuery,
                            fieldInfoMap.get(fieldName), this.allFields))),
                    () -> tableWithAlias(diyTableQuery, diyTableNewTableName),
                    null
            );

        } else {
            String newTableName = nextTableAlias();
            SQLSelectQueryBlock diyTableQuery = setDiyTableQuery(joinTable.getTableId());
            getCurrFieldList().forEach(f -> queryBlock.addSelectItem(
                    fullField(newTableName, unQuote(getCurrentColumnAliasWithFuncPrefix(diyTableQuery, f, this.allFields))),
                    quote(nextColumnAlias())
            ));

            SQLJoinTableSource joinTableSource = new SQLJoinTableSource();
            joinTableSource.setJoinType(SQLJoinTableSource.JoinType.valueOf(joinTypeEnum.name()));
            joinTableSource.setLeft(queryBlock.getFrom());
            joinTableSource.setRight(tableWithAlias(diyTableQuery, newTableName));

            Map<String, FieldInfo> fieldInfoMap = this.diyTableCurrFields.get(joinTable.getTableId()).stream().collect(Collectors.toMap(FieldInfo::getRealTransferName, f -> f, (f1, f2) -> f1));
            List<List<String>> basis = joinValue.getBasis();
            List<SQLBinaryOpExpr> joinConditions = basis.stream().map(c ->
                    joinCondition(
                            this.currFields.get(c.get(0)).fieldExpr,
                            fullField(newTableName, unQuote(getCurrentColumnAliasWithFuncPrefix(diyTableQuery,
                                    fieldInfoMap.get(c.get(1)), this.allFields))))
            ).collect(Collectors.toList());
            joinTableSource.setCondition(multiJoinCondition(joinConditions));

            queryBlock.setFrom(joinTableSource);

            this.firstJoin = true;
        }
    }

    private void joinDb(SQLSelectQueryBlock queryBlock, JoinOperator.JoinInfo joinValue) {
        JoinTypeEnum joinTypeEnum = JoinTypeEnum.convertByCode(joinValue.getStyle());

        SelectFieldOperator.Fields joinTable = joinValue.getTable();
        String joinTableName = getTableName(joinTable.getTableName());

        if (needSubQuery) {

            subQueryJoin(
                    queryBlock,
                    joinValue,
                    fieldName -> fullField(joinTableName, fieldName),
                    () -> tableNameWithAlias(joinTable.getTableName(), joinTableName),
                    joinTableName
            );

        } else {
            joinTable.getFields().forEach(f -> queryBlock.addSelectItem(fullField(joinTableName, f.getFieldName()), quote(nextColumnAlias())));

            SQLJoinTableSource joinTableSource = new SQLJoinTableSource();
            joinTableSource.setJoinType(SQLJoinTableSource.JoinType.valueOf(joinTypeEnum.name()));
            joinTableSource.setLeft(queryBlock.getFrom());
            joinTableSource.setRight(tableNameWithAlias(joinTable.getTableName(), joinTableName));

            List<List<String>> basis = joinValue.getBasis();
            List<SQLBinaryOpExpr> joinConditions = basis.stream().map(c ->
                    joinCondition(
                            this.currFields.get(c.get(0)).fieldExpr,
                            fullField(joinTableName, c.get(1)))
            ).collect(Collectors.toList());
            joinTableSource.setCondition(multiJoinCondition(joinConditions));

            queryBlock.setFrom(joinTableSource);

            fillPermissionPlaceHolder(queryBlock, joinTable, joinTableName);

            this.firstJoin = true;
        }
    }

    private void fillPermissionPlaceHolder(SQLSelectQueryBlock queryBlock, SelectFieldOperator.Fields joinTable, String joinTableName) {
        String placeHolderName = generateInnerTablePermissionPlaceHolderName(joinTableName);
        this.relatedTables.add(
                new RelationTableInfo(joinTable.getTableId(), placeHolderName)
        );
        if (this.buildWithPlaceHolder) {
            beforeAddWhere(queryBlock);
            queryBlock.addWhere(new SQLIdentifierExpr(generatePlaceholder(placeHolderName)));
        }
    }

    private void groupByFillFields(GroupByOperator groupBy) {
        List<GroupByOperator.GroupByFieldInfo> dimensions = groupBy.getValue().getDimensions();
        List<GroupByOperator.GroupByFieldInfo> norms = groupBy.getValue().getNorms();

        this.currFields.clear();
        this.currSortedFieldIds.clear();
        dimensions.forEach(f -> {
            FieldInfo fieldInfo = addFuncField(f.getFieldId(), f.getTransferName(), FieldTypeEnum.convertByCode(f.getFieldType()), Objects.isNull(f.getFunctionType()) ? 0 : f.getFunctionType(), true, false, false);
            this.currFields.put(fieldInfo.fieldId, fieldInfo);
            this.currSortedFieldIds.add(fieldInfo.fieldId);
        });
        norms.forEach(f -> {
            FieldInfo fieldInfo = addFuncField(f.getFieldId(), f.getTransferName(), FieldTypeEnum.convertByCode(f.getFieldType()), f.getFunctionType(), false, true, false);
            this.currFields.put(fieldInfo.fieldId, fieldInfo);
            this.currSortedFieldIds.add(fieldInfo.fieldId);
        });
    }

    private void doGroupBy(GroupByOperator groupBy, SQLSelectQueryBlock queryBlock) {
        SQLSelectQueryBlock oldQueryBlock = new SQLSelectQueryBlock();
        queryBlock.cloneTo(oldQueryBlock);

        SQLSelectQueryBlock newQueryBlock = new SQLSelectQueryBlock();
        String newTableName = nextTableAlias();

        GroupByOperator.GroupInfo groupByValue = groupBy.getValue();
        List<GroupByOperator.GroupByFieldInfo> dimensions = groupByValue.getDimensions();
        List<GroupByOperator.GroupByFieldInfo> norms = groupByValue.getNorms();

        Map<GroupByOperator.GroupByFieldInfo, FieldInfo> usedColumns = new HashMap<>(dimensions.size() + norms.size());
        dimensions.forEach(d -> usedColumns.put(d, this.allFields.get(d.getFieldId())));
        norms.forEach(n -> usedColumns.put(n, this.allFields.get(n.getFieldId())));

        Map<FieldInfo, String> currentColumnAlias = getCurrentColumnAliasWithFuncPrefix(oldQueryBlock, new HashSet<>(usedColumns.values()), this.allFields);


        dimensions.forEach(d -> {
            if (d.getFieldType() == FieldTypeEnum.DATE.getCode()) {
                // 设置默认值
                DateFunctionEnum functionEnum = DateFunctionEnum.convertByCode(Objects.isNull(d.getFunctionType()) ? 1 : d.getFunctionType());
                newQueryBlock.addSelectItem(
                        simpleFunctionField(newTableName, unQuote(currentColumnAlias.get(usedColumns.get(d))),
                                functionEnum.getFunctionFormat(), functionEnum.getArgCount()),
                        quote(nextColumnAlias()));
            } else {
                newQueryBlock.addSelectItem(fullField(newTableName, unQuote(currentColumnAlias.get(usedColumns.get(d)))), quote(nextColumnAlias()));
            }
        });
        norms.forEach(n -> {
            if (n.getFieldType() == FieldTypeEnum.DATE.getCode()) {
                DateAggFuncEnum funcEnum = DateAggFuncEnum.convertByCode(n.getFunctionType());
                newQueryBlock.addSelectItem(
                        simpleFunctionField(newTableName, unQuote(currentColumnAlias.get(usedColumns.get(n))),
                                funcEnum.getFunctionFormat(), funcEnum.getArgCount()),
                        quote(nextColumnAlias()));
            } else if (n.getFieldType() == FieldTypeEnum.STRING.getCode()) {
                TextAggFuncEnum funcEnum = TextAggFuncEnum.convertByCode(n.getFunctionType());
                newQueryBlock.addSelectItem(
                        simpleFunctionField(newTableName, unQuote(currentColumnAlias.get(usedColumns.get(n))),
                                funcEnum.getFunctionFormat(), funcEnum.getArgCount()),
                        quote(nextColumnAlias()));
            } else if (n.getFieldType() == FieldTypeEnum.NUMBER.getCode()) {
                NumberAggFuncEnum funcEnum = NumberAggFuncEnum.convertByCode(n.getFunctionType());
                newQueryBlock.addSelectItem(
                        simpleFunctionField(newTableName, unQuote(currentColumnAlias.get(usedColumns.get(n))),
                                funcEnum.getFunctionFormat(), funcEnum.getArgCount()),
                        quote(nextColumnAlias()));
            } else {
                throw new IllegalArgumentException("不支持的字段类型");
            }
        });

        newQueryBlock.setFrom(tableWithAlias(oldQueryBlock, newTableName));

        newQueryBlock.setGroupBy(generateGroupBy(dimensions.size()));

        queryBlock.setWhere(null);
        newQueryBlock.cloneTo(queryBlock);

        this.needSubQuery = true;
    }

    private void setFieldFillFields(SetFieldOperator setField) {
        List<SetFieldOperator.FieldSetters> fieldSetters = setField.getValue().getFieldList();

        this.currFields.clear();
        this.currSortedFieldIds.clear();
        fieldSetters.forEach(f -> {
            if (f.getUsed()) {
                FieldInfo fieldInfo;
                if (f.getTypeSpecified()) {
                    fieldInfo = addFuncField(f.getFieldId(), f.getTransferName(), FieldTypeEnum.convertByCode(f.getFieldType()), 0, false, false, true);
                } else {
                    fieldInfo = this.allFields.get(f.getFieldId());
                    fieldInfo.transferName = f.getTransferName();
                    fieldInfo.repeatCount = getTransferNameRepeatCount(f.getTransferName());
                }
                this.currFields.put(fieldInfo.getFieldId(), fieldInfo);
                this.currSortedFieldIds.add(fieldInfo.fieldId);
            }
        });
    }

    private void doSetField(SetFieldOperator setField, SQLSelectQueryBlock queryBlock) {
        List<SetFieldOperator.FieldSetters> setFieldValue = setField.getValue().getFieldList();

        Map<SetFieldOperator.FieldSetters, FieldInfo> fieldInfos = setFieldValue.stream().collect(Collectors.toMap(f -> f, f -> this.allFields.get(f.getFieldId()), (o1, o2) -> o1));
        Map<FieldInfo, String> currentColumnAlias = getCurrentColumnAliasWithFuncPrefix(queryBlock, new HashSet<>(fieldInfos.values()), this.allFields);

        List<SetFieldOperator.FieldSetters> specifiedFields = setFieldValue.stream().filter(f -> f.getTypeSpecified() || !f.getUsed()).collect(Collectors.toList());
        Map<SetFieldOperator.FieldSetters, FieldInfo> specifiedFieldExpr = specifiedFields.stream().collect(Collectors.toMap(f -> f, f -> this.allFields.get(f.getFieldId()), (o1, o2) -> o1));

        List<SQLSelectItem> selectList = queryBlock.getSelectList();
        int specifiedTypeFieldCount = 0;
        for (int i = 0; i < selectList.size(); i++) {
            SQLSelectItem selectItem = selectList.get(i);
            for (SetFieldOperator.FieldSetters fieldSetters : specifiedFieldExpr.keySet()) {
                if (selectItem.getAlias().equals(currentColumnAlias.get(specifiedFieldExpr.get(fieldSetters)))) {
                    if (!fieldSetters.getUsed()) {
                        selectList.remove(i--);
                    } else if (fieldSetters.getTypeSpecified()) {
                        specifiedTypeFieldCount++;
                        FieldTypeEnum fieldTypeEnum = FieldTypeEnum.convertByCode(fieldSetters.getFieldType());
                        FieldTypeTransformEnum fieldTypeTransformEnum = FieldTypeTransformEnum.convertByToType(fieldInfos.get(fieldSetters).getFieldType().getCode(), fieldTypeEnum.getCode());
                        selectItem.setExpr(wrapFieldWithFunction(selectItem.getExpr(), fieldTypeTransformEnum.getFunctionFormat(), fieldTypeTransformEnum.getArgCount()));
                    }
                }
            }
        }

        List<SQLSelectItem> sortedSelectList = new ArrayList<>(selectList.size());
        setFieldValue.forEach(f -> {
            if (f.getUsed()) {
                String currAlias = currentColumnAlias.get(fieldInfos.get(f));
                sortedSelectList.add(selectList.stream().filter(i -> currAlias.equals(i.getAlias())).findFirst().orElseThrow(() -> new IllegalArgumentException("set_field sorted field not found.")));
            }
        });
        selectList.clear();
        selectList.addAll(sortedSelectList);

        this.needSubQuery = specifiedTypeFieldCount > 0 || this.needSubQuery;
    }

    private void doFilter(FilterOperator filter, SQLSelectQueryBlock queryBlock) {
        FilterOperator.Filters<Object> filterValue = filter.getValue();

        Integer[] conditionFilterTypes = FilterTypeEnum.getConditionFilterTypes().stream().map(FilterTypeEnum::getCode).toArray(Integer[]::new);
        filter.transferFilterValue(filterValue, conditionFilterTypes, this.objectMapper);

        if (!firstJoin && needSubQuery) {
            SQLSelectQueryBlock oldQueryBlock = new SQLSelectQueryBlock();
            queryBlock.cloneTo(oldQueryBlock);

            SQLSelectQueryBlock newQueryBlock = new SQLSelectQueryBlock();
            String newTableName = nextTableAlias();

            oldQueryBlock.getSelectList().forEach(f -> newQueryBlock.addSelectItem(fullField(newTableName, unQuote(f.getAlias())), quote(nextColumnAlias())));

            newQueryBlock.addWhere(new SQLIdentifierExpr("1 = 1"));
            newQueryBlock.addWhere(generateWhere(filterValue, oldQueryBlock, newTableName));

            newQueryBlock.setFrom(tableWithAlias(oldQueryBlock, newTableName));

            queryBlock.setGroupBy(null);
            newQueryBlock.cloneTo(queryBlock);

        } else {
            queryBlock.addWhere(new SQLIdentifierExpr("1 = 1"));
            queryBlock.addWhere(generateWhere(filterValue, queryBlock, null));
        }

        this.needSubQuery = true;
    }

    private SQLExpr generateWhere(FilterOperator.Filters<Object> filter, SQLSelectQueryBlock queryBlock, String tableName) {
        FilterTypeEnum filterTypeEnum = FilterTypeEnum.convertByCode(filter.getFilterType());
        String fieldId = filter.getFieldId();
        SQLExpr fieldExpr = null;
        FieldInfo fieldInfo = null;
        if (StringUtils.hasText(fieldId)) {
            fieldInfo = this.allFields.get(fieldId);
            fieldExpr = !firstJoin && needSubQuery ? fullField(tableName, unQuote(getCurrentColumnAliasWithFuncPrefix(queryBlock, fieldInfo, this.allFields))) : fieldInfo.fieldExpr;
        } else if (!FilterTypeEnum.getConditionFilterTypes().contains(filterTypeEnum)) {
            throw new IllegalArgumentException("fieldId could not be null.");
        }

        switch (filterTypeEnum) {
            case AND:
            case OR:
                @SuppressWarnings("unchecked") List<FilterOperator.Filters<Object>> filters = (List<FilterOperator.Filters<Object>>) filter.getFilterValue();
                List<SQLExpr> filterExprList = filters.stream().map(f -> generateWhere(f, queryBlock, tableName)).collect(Collectors.toList());
                SQLExpr expr = null;
                for (SQLExpr sqlExpr : filterExprList) {
                    if (Objects.isNull(expr)) {
                        expr = sqlExpr;
                    } else {
                        expr =  sqlExpr != null ? new SQLBinaryOpExpr(expr, filterTypeEnum.getOps()[0], sqlExpr) : expr;
                    }
                }
                return expr;
            case BETWEEN:
            case NOT_BETWEEN:
                NumberBetween numberBetween = this.objectMapper.convertValue(filter.getFilterValue(), NumberBetween.class);
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
                            numberBetweenMaxExpr : new SQLBinaryOpExpr(numberBetweenExpr, filterTypeEnum.getOps()[2], numberBetweenMaxExpr);
                }
                return numberBetweenExpr;
            case EQUALS:
            case NOT_EQUALS:
            case GREAT_THAN:
            case LESS_THAN:
            case GREAT_THAN_EQUALS:
            case LESS_THAN_EQUALS:
                return new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[0], new SQLIdentifierExpr(String.valueOf(filter.getFilterValue())));
            case IS_NULL:
            case NOT_NULL:
            case DATE_IS_NULL:
            case DATE_NOT_NULL:
                return new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[0], new SQLNullExpr());
            case BELONG:
                ArrayList<StringBelong> filterInValue = this.objectMapper.convertValue(filter.getFilterValue(), new TypeReference<ArrayList<StringBelong>>() {
                });
                SQLInListExpr sqlInListExpr = new SQLInListExpr(fieldExpr);
                filterInValue.forEach(v -> sqlInListExpr.addTarget(new SQLCharExpr(v.getValue())));
                return sqlInListExpr;
            case NOT_BELONG:
                ArrayList<StringBelong> filterNotInValue = this.objectMapper.convertValue(filter.getFilterValue(), new TypeReference<ArrayList<StringBelong>>() {
                });
                SQLInListExpr sqlNotInListExpr = new SQLInListExpr(fieldExpr, true);
                filterNotInValue.forEach(v -> sqlNotInListExpr.addTarget(new SQLCharExpr(v.getValue())));
                return sqlNotInListExpr;
            case CONTAIN:
            case NOT_CONTAIN:
                return new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[0], new SQLCharExpr("%" + filter.getFilterValue() + "%"));
            case TEXT_IS_NULL:
            case TEXT_NOT_NULL:
                return new SQLBinaryOpExpr(
                        new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[0], new SQLCharExpr("")),
                        filterTypeEnum.getOps()[1],
                        new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[2], new SQLNullExpr())
                );
            case START_WITH:
            case START_NOT_WITH:
                return new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[0], new SQLCharExpr(filter.getFilterValue() + "%"));
            case END_WITH:
            case END_NOT_WITH:
                return new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[0], new SQLCharExpr("%" + filter.getFilterValue()));
            case DATE_BELONG:
            case DATE_NOT_BELONG:
                DateRangeDay dateRangeDay = this.objectMapper.convertValue(filter.getFilterValue(), DateRangeDay.class);
                LocalDate startTime = LocalDate.parse(dateRangeDay.getStartTime(), YYYY_MM_DD);
                LocalDate endTime = LocalDate.parse(dateRangeDay.getEndTime(), YYYY_MM_DD);
                LocalDate endTimePlus = endTime.plusDays(1);
                return new SQLBinaryOpExpr(
                        new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[0], new SQLTimestampExpr(Date.from(startTime.atStartOfDay(ZoneId.systemDefault()).toInstant()))),
                        filterTypeEnum.getOps()[2],
                        new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[1], new SQLTimestampExpr(Date.from(endTimePlus.atStartOfDay(ZoneId.systemDefault()).toInstant())))
                );
            case DATE_EQUALS:
            case DATE_NOT_EQUALS:
                DateSimpleDay dateSimpleDay = this.objectMapper.convertValue(filter.getFilterValue(), DateSimpleDay.class);
                LocalDate date = LocalDate.parse(dateSimpleDay.getDate(), YYYY_MM_DD);
                LocalDate datePlus = date.plusDays(1);
                return new SQLBinaryOpExpr(
                        new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[0], new SQLTimestampExpr(Date.from(date.atStartOfDay(ZoneId.systemDefault()).toInstant()))),
                        filterTypeEnum.getOps()[2],
                        new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[1], new SQLTimestampExpr(Date.from(datePlus.atStartOfDay(ZoneId.systemDefault()).toInstant())))
                );
            case DATE_BEFORE:
                DateSimpleDay beforeDateSimpleDay = this.objectMapper.convertValue(filter.getFilterValue(), DateSimpleDay.class);
                LocalDate beforeDate = LocalDate.parse(beforeDateSimpleDay.getDate(), YYYY_MM_DD);
                return new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[0], new SQLTimestampExpr(Date.from(beforeDate.plusDays(1).atStartOfDay(ZoneId.systemDefault()).toInstant())));
            case DATE_AFTER:
                DateSimpleDay afterDateSimpleDay = this.objectMapper.convertValue(filter.getFilterValue(), DateSimpleDay.class);
                LocalDate afterDate = LocalDate.parse(afterDateSimpleDay.getDate(), YYYY_MM_DD);
                return new SQLBinaryOpExpr(fieldExpr, filterTypeEnum.getOps()[0], new SQLTimestampExpr(Date.from(afterDate.atStartOfDay(ZoneId.systemDefault()).toInstant())));
            case PLACEHOLDER:
                String placeHolderName = (String) filter.getFilterValue();
                this.placeHolderInfos.add(
                        new PlaceHolderInfo(placeHolderName, SQLUtils.toSQLString(fieldExpr), fieldInfo.fieldType, fieldId)
                );
                return buildWithPlaceHolder ? new SQLIdentifierExpr(generatePlaceholder(fieldId)) : null;
            default:
                throw new IllegalArgumentException("unsupported operator.");
        }
    }

    /**
     * 获取内部表名称，可以重复调用
     * @param tableName 表名
     * @return 内部表名
     */
    private String getTableName(String tableName) {
        if (!tableNameMappings.containsKey(tableName)) {
            tableNameMappings.put(tableName, generateTableName(tableName));
        }
        return tableNameMappings.get(tableName);
    }

    /**
     * 生成子查询表别名
     * @return 子查询表别名
     */
    private String nextTableAlias() {
        return TABLE_ALIAS_PREFIX + tableIndex++;
    }

    /**
     * 生成列别名
     * @return 列别名
     */
    private String nextColumnAlias() {
        return COLUMN_ALIAS_PREFIX + columnIndex++;
    }

    private void beforeAddWhere(SQLSelectQueryBlock queryBlock) {
        if (queryBlock.getWhere() == null) {
            queryBlock.addWhere(new SQLIdentifierExpr("1 = 1"));
        }
    }

    /**
     * 生成自助数据集类型的表对应的查询
     * @param tableId 表名称
     * @return 查询内容
     */
    private SQLSelectQueryBlock setDiyTableQuery(Long tableId) {
        if (Objects.isNull(this.getTableOperators)) {
            throw new IllegalArgumentException("getTableOperators is null.");
        }
        if (!this.diyTableQueryMappings.containsKey(tableId)) {
            SQLSelectQueryBlock queryBlock = buildSqlWithOperators(this.getTableOperators.apply(tableId));
            this.diyTableQueryMappings.put(tableId, queryBlock);
            this.diyTableCurrFields.put(tableId, new ArrayList<>(getCurrFieldList()));
        }
        return this.diyTableQueryMappings.get(tableId);
    }

    private FieldInfo addField(String fieldName, FieldTypeEnum fieldType, SQLExpr fieldExpr) {
        return addField(fieldName, fieldName, fieldType, fieldExpr);
    }

    private FieldInfo addField(String fieldName, String transferName, FieldTypeEnum fieldType, SQLExpr fieldExpr) {
        int repeatCount = getTransferNameRepeatCount(transferName);
        FieldInfo field = new FieldInfo(fieldType, fieldExpr, fieldName, transferName, repeatCount);
        field.fieldId = generateFieldId(field);

        this.allFields.put(field.fieldId, field);

        return field;
    }

    /**
     * 返回字段名称在当前查询列表中重复的次数
     * @param transferName 字段名称
     * @return 当前重复次数
     */
    private int getTransferNameRepeatCount(String transferName) {
        int maxRepeatCount = 0;
        boolean repeatFlag = false;
        for (FieldInfo fieldInfo : this.currFields.values()) {
            if (fieldInfo.transferName.equals(transferName)) {
                repeatFlag = true;
                maxRepeatCount = Math.max(maxRepeatCount, fieldInfo.repeatCount);
            }
        }
        return repeatFlag ? ++maxRepeatCount : maxRepeatCount;
    }

    private FieldInfo addFuncField(String sourceFieldId, String transferName, FieldTypeEnum fieldType, int functionType, boolean isDimension, boolean isNorm, boolean typeSpecified) {
        int repeatCount = getTransferNameRepeatCount(transferName);
        FieldInfo field = new FieldInfo(fieldType, this.allFields.get(sourceFieldId).fieldExpr, null, transferName, repeatCount);
        field.sourceFieldId = sourceFieldId;
        field.functionType = functionType;
        field.hasFunction = true;
        field.isDimension = isDimension;
        field.isNorm = isNorm;
        field.typeSpecified = typeSpecified;
        field.fieldId = generateFieldId(field);

        this.allFields.put(field.fieldId, field);

        return field;
    }

    /**
     * 生成全局唯一fieldId
     * 一个OperatorSqlAdapter实例中不能重复
     * @param fieldInfo 字段信息
     * @return 全局唯一fieldId
     */
    private String generateFieldId(FieldInfo fieldInfo) {
        StringBuilder sb = new StringBuilder();
        while (!fieldInfo.isSourceField()) {
            sb.append(fieldInfo.getFunctionPrefix());
            sb.append(fieldInfo.sourceFieldId);
            fieldInfo = this.allFields.get(fieldInfo.sourceFieldId);
        }
        sb.append(SQLUtils.toSQLString(fieldInfo.fieldExpr));
        return DigestUtils.md5DigestAsHex(sb.toString().getBytes());
    }

    @Data
    public static class FieldInfo {
        /**
         * 全局唯一fieldId
         */
        private String fieldId;
        private FieldTypeEnum fieldType;
        /**
         * 源字段表达式
         */
        @JsonIgnoreProperties
        private SQLExpr fieldExpr;
        private String fieldName;
        /**
         * 在当前查询字段列表中的名称
         */
        private String transferName;
        /**
         * 在当前查询字段列表中名称重复的次数
         */
        private int repeatCount;

        /**
         * 来源字段id
         */
        private String sourceFieldId;
        private int functionType;
        private boolean hasFunction;
        private boolean isDimension;
        private boolean isNorm;

        private boolean typeSpecified;

        public FieldInfo(FieldTypeEnum fieldType, SQLExpr fieldExpr, String fieldName, String transferName, int repeatCount) {
            this.fieldType = fieldType;
            this.fieldExpr = fieldExpr;
            this.fieldName = fieldName;
            this.transferName = transferName;
            this.repeatCount = repeatCount;
            this.hasFunction = false;
            this.isDimension = false;
            this.isNorm = false;
            this.typeSpecified = false;
        }

        public String getFieldExpr() {
            return SQLUtils.toSQLString(fieldExpr);
        }

        /**
         * 名称+重复次数=当前查询字段列表唯一名称
         * @return 名称
         */
        public String getRealTransferName() {
            return this.repeatCount == 0 ? this.transferName : this.transferName + this.repeatCount;
        }

        public boolean isSourceField() {
            return !this.hasFunction
                    && !this.isDimension
                    && !this.isNorm
                    && !this.typeSpecified;
        }

        /**
         * 获取字段前缀
         * @return 当前字段前缀
         */
        public String getFunctionPrefix() {
            if (!this.hasFunction) {
                return "";
            }
            if (this.isDimension) {
                if (this.fieldType == FieldTypeEnum.DATE) {
                    DateFunctionEnum functionEnum = DateFunctionEnum.convertByCode(Math.max(this.functionType, 1));
                    return functionEnum.getFunctionFormat();
                } else {
                    return "";
                }
            } else if (this.isNorm) {
                switch (this.fieldType) {
                    case DATE:
                        DateAggFuncEnum dateAggFuncEnum = DateAggFuncEnum.convertByCode(this.functionType);
                        return dateAggFuncEnum.getFunctionFormat();
                    case STRING:
                        TextAggFuncEnum textAggFuncEnum = TextAggFuncEnum.convertByCode(this.functionType);
                        return textAggFuncEnum.getFunctionFormat();
                    case NUMBER:
                        NumberAggFuncEnum numberAggFuncEnum = NumberAggFuncEnum.convertByCode(this.functionType);
                        return numberAggFuncEnum.getFunctionFormat();
                    default:
                        throw new IllegalArgumentException("getRealFieldExpr 不支持的字段类型");
                }
            } else if (this.typeSpecified) {
                FieldTypeTransformEnum fieldTypeTransformEnum = FieldTypeTransformEnum.convertByToType(this.fieldType.getCode(), this.fieldType.getCode());
                return fieldTypeTransformEnum.getFunctionFormat();
            } else {
                throw new IllegalArgumentException("getFunctionPrefix error.");
            }
        }

    }

}
