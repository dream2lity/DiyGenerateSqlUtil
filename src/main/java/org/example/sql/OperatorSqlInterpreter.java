package org.example.sql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.util.JdbcConstants;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.example.enums.FieldTypeEnum;
import org.example.sql.base.*;
import org.example.sql.interpreter.OperatorInterpreter;
import org.example.sql.interpreter.OperatorInterpreterFactory;
import org.example.sql.operator.AnalysisOperator;
import org.example.sql.operator.SelectFieldOperator;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.function.Function;

import static org.example.sql.SqlBuildHelper.generatePlaceholder;
import static org.example.sql.SqlBuildHelper.generateTableName;

/**
 * @author chijiuwang
 */
@Slf4j
@Data
public class OperatorSqlInterpreter {

    private static final String TABLE_ALIAS_PREFIX = "t";
    private static final String COLUMN_ALIAS_PREFIX = "__col_";

    /**
     * 表别名后缀， 在当前查询中的序号
     * <p>
     * TABLE_ALIAS_PREFIX+tableIndex=表别名
     */
    private int tableIndex = 0;
    /**
     * 字段别名后缀， 在当前查询中的序号
     * <p>
     * COLUMN_ALIAS_PREFIX+columnIndex=字段别名
     */
    private int columnIndex = 0;
    /**
     * 单表允许的最大行数
     */
    private int tableRowCount;
    /**
     * 是否限制表查询行数，默认设置，根据 tableRowCount 决定
     */
    private boolean isTableLimit;

    /**
     * 最终SQL可以追加limit参数， 根据 offset 和 rowCount 确定
     * <p>
     *    需要调用 setOffset 单独设置
     * </p>
     */
    private Integer offset;
    private Integer rowCount;
    /**
     * 缓存表名映射，真实表名-内部生成的表名
     */
    private final Map<OperatorTable, String> tableNameMappings = new HashMap<>();

    /**
     * 缓存自助数据集表名和查询映射，自助数据集表名-查询
     */
    private final Map<Long, SQLSelectQueryBlock> diyTableQueryMappings = new HashMap<>();
    /**
     * 缓存自助数据集表名和字段列表映射，自助数据集表名-字段列表
     */
    private final Map<Long, List<OperatorField>> diyTableFields = new HashMap<>();
    /**
     * 获取表operators的方法
     */
    private Function<Long, List<AnalysisOperator<?>>> getTableOperators;
    private boolean needSubQuery;
    private boolean firstJoin;

    private boolean buildWithPlaceHolder;
    private final List<PlaceHolderInfo> placeHolderInfos = new ArrayList<>();
    private final Set<RelationTableInfo> relatedTables = new HashSet<>();

    private List<OperatorCheckResult> operatorCheckResults = new ArrayList<>();
    private List<OperatorField> checkFields = new ArrayList<>();

    private final ObjectMapper objectMapper;

    private final List<AnalysisOperator<?>> operators;
    private SQLSelectQueryBlock queryBlock = new SQLSelectQueryBlock();
    private List<OperatorField> fields = new ArrayList<>();
    private List<OperatorTable> tables = new ArrayList<>();

    public OperatorSqlInterpreter(ObjectMapper objectMapper, List<AnalysisOperator<?>> operators) {
        this.objectMapper = objectMapper;
        this.operators = operators;
        this.isTableLimit = false;
        this.buildWithPlaceHolder = false;
    }

    public void setTableRowCount(int tableRowCount) {
        this.tableRowCount = tableRowCount;
        this.isTableLimit = true;
    }

    /**
     * 设置最终的分页参数
     * @param offset 偏移量
     * @param rowCount 行数
     */
    public void setOffset(int offset, int rowCount) {
        this.offset = offset;
        this.rowCount = rowCount;
    }

    public void checkOperators() {
        if (CollectionUtils.isEmpty(this.operators)) {
            throw new IllegalArgumentException("operators could not empty.");
        }
        if (this.operators.size() > 0 && !(this.operators.get(0) instanceof SelectFieldOperator)) {
            throw new IllegalArgumentException("first operator must be select_field.");
        }
        this.operators.forEach(operator ->
            OperatorInterpreterFactory.getInterpreter(operator, this).checkOperators()
        );
    }

    public void generateFieldList() {
        for (int i = 0; i < this.operators.size(); i++) {
            try {
                OperatorInterpreter<?> interpreter = OperatorInterpreterFactory.getInterpreter(this.operators.get(i), this);
                interpreter.checkOperator();
                interpreter.generateFieldList();
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format("请检查第%s步[%s]", i + 1, e.getMessage()));
            }
        }
    }

    public void buildSql() {
        if (isTableLimit) {
            queryBlock.limit(tableRowCount, 0);
        }
        for (int i = 0; i < this.operators.size(); i++) {
            try {
                OperatorInterpreter<?> interpreter = OperatorInterpreterFactory.getInterpreter(this.operators.get(i), this);
                interpreter.checkOperator();
                interpreter.generateSql();
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(String.format("请检查第%s步[%s]", i + 1, e.getMessage()));
            }
        }
        if (Objects.nonNull(this.offset) && Objects.nonNull(this.rowCount)) {
            queryBlock.limit(this.rowCount, this.offset);
        }
    }

    public String getSql() {
        return SQLUtils.toSQLString(this.queryBlock, JdbcConstants.MYSQL);
    }

    public void fillPermissionPlaceHolder(SQLSelectQueryBlock queryBlock, Long tableId, String tableName) {
        this.relatedTables.add(
                new RelationTableInfo(tableId, tableName)
        );
        if (this.buildWithPlaceHolder) {
            beforeAddWhere(queryBlock);
            queryBlock.addWhere(new SQLIdentifierExpr(generatePlaceholder(tableName)));
        }
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
    public SQLSelectQueryBlock setDiyTableQuery(Long tableId) {
        if (Objects.isNull(this.getTableOperators)) {
            throw new IllegalArgumentException("getTableOperators is null.");
        }
        if (!this.diyTableQueryMappings.containsKey(tableId)) {
            OperatorSqlInterpreter diyTableOperatorSqlInterpreter = new OperatorSqlInterpreter(objectMapper, this.getTableOperators.apply(tableId));
            diyTableOperatorSqlInterpreter.setGetTableOperators(this.getTableOperators);
            diyTableOperatorSqlInterpreter.setBuildWithPlaceHolder(this.buildWithPlaceHolder);
            if (this.isTableLimit) {
                diyTableOperatorSqlInterpreter.setTableRowCount(this.tableRowCount);
            }
            diyTableOperatorSqlInterpreter.setTableIndex(this.tableIndex);
            diyTableOperatorSqlInterpreter.setColumnIndex(this.columnIndex);
            diyTableOperatorSqlInterpreter.buildSql();
            SQLSelectQueryBlock queryBlock = diyTableOperatorSqlInterpreter.getQueryBlock();
            this.diyTableQueryMappings.put(tableId, queryBlock);
            this.diyTableFields.put(tableId, new ArrayList<>(diyTableOperatorSqlInterpreter.getFields()));
            this.tableIndex = diyTableOperatorSqlInterpreter.getTableIndex();
            this.columnIndex = diyTableOperatorSqlInterpreter.getColumnIndex();
            this.placeHolderInfos.addAll(diyTableOperatorSqlInterpreter.getPlaceHolderInfos());
            this.relatedTables.addAll(diyTableOperatorSqlInterpreter.getRelatedTables());
        }
        return this.diyTableQueryMappings.get(tableId);
    }

    public Map<String, OperatorField> aliasFieldMapping() {
        Map<String, OperatorField> map = new HashMap<>(this.fields.size());
        for (OperatorField field : this.fields) {
            map.put(field.getAlias(), field);
        }
        return map;
    }

    public void addCheckField(FieldTypeEnum fieldType, String transferName, OperatorField sourceField) {
        OperatorField newField = getNewField(transferName, this.checkFields);
        this.checkFields.add(new OperatorField(fieldType, newField.getTransferName(), newField.getRepeatCount(), sourceField));
    }

    public void clearCheckFields() {
        this.checkFields.clear();
    }

    public void addField(FieldTypeEnum fieldType, String transferName, OperatorField sourceField) {
        OperatorField newField = getNewField(transferName, this.fields);
        this.fields.add(new OperatorField(fieldType, newField.getTransferName(), newField.getRepeatCount(), sourceField));
    }

    public void addField(FieldTypeEnum fieldType, String transferName, SQLExpr sqlExpr, String alias, OperatorField sourceField) {
        OperatorField newField = getNewField(transferName, this.fields);
        this.fields.add(new OperatorField(fieldType, newField.getTransferName(), newField.getRepeatCount(), sqlExpr, alias, sourceField));
    }

    private OperatorField getNewField(String transferName, List<OperatorField> fields) {
        OperatorField newField = new OperatorField();
        newField.setRepeatCount(getTransferNameRepeatCount(transferName, fields));
        newField.setTransferName(transferName);
        int takeTimes = 0;
        while (fields.stream().anyMatch(f -> newField.getRealTransferName().equals(f.getRealTransferName()))) {
            String realTransferName = newField.getRealTransferName();
            newField.setTransferName(realTransferName);
            int transferNameRepeatCount = getTransferNameRepeatCount(realTransferName, fields);
            newField.setRepeatCount(transferNameRepeatCount > 0 ? transferNameRepeatCount : 1);
            takeTimes++;
            if (takeTimes > fields.size()) {
                throw new RuntimeException("getNewField time out.");
            }
        }
        return newField;
    }

    public void clearFields() {
        this.fields.clear();
    }

    /**
     * 返回字段名称在当前查询列表中重复的次数
     * @param transferName 字段名称
     * @return 当前重复次数
     */
    public int getTransferNameRepeatCount(String transferName, List<OperatorField> fields) {
        int maxRepeatCount = 0;
        boolean repeatFlag = false;
        for (OperatorField field: fields) {
            if (field.getTransferName().equals(transferName)) {
                repeatFlag = true;
                maxRepeatCount = Math.max(maxRepeatCount, field.getRepeatCount());
            }
        }
        return repeatFlag ? ++maxRepeatCount : maxRepeatCount;
    }

    /**
     * 获取内部表名称，可以重复调用
     * @param table 表名
     * @return 内部表名
     */
    public String getTableName(OperatorTable table) {
        if (!tableNameMappings.containsKey(table)) {
            tableNameMappings.put(table, generateTableName(table.getInnerTableName()));
        }
        return tableNameMappings.get(table);
    }

    /**
     * 添加DB数据集到当前上下文中
     * @param tableName DB数据集表名称
     * @return 上下文中DB数据集对应的表
     */
    public OperatorTable addDbTable(String tableName) {
        OperatorTable operatorTable = new OperatorTable(tableName, getDbTableNameRepeatCount(tableName, this.tables));
        this.tables.add(operatorTable);
        return operatorTable;
    }

    private int getDbTableNameRepeatCount(String tableName, List<OperatorTable> tables) {
        int maxRepeatCount = 0;
        boolean repeatFlag = false;
        for (OperatorTable table: tables) {
            if (table.getTableName().equals(tableName)) {
                repeatFlag = true;
                maxRepeatCount = Math.max(maxRepeatCount, table.getRepeatCount());
            }
        }
        return repeatFlag ? ++maxRepeatCount : maxRepeatCount;
    }

    /**
     * 生成子查询表别名
     * @return 子查询表别名
     */
    public String nextTableAlias() {
        return TABLE_ALIAS_PREFIX + tableIndex++;
    }

    /**
     * 生成列别名
     * @return 列别名
     */
    public String nextColumnAlias() {
        return COLUMN_ALIAS_PREFIX + columnIndex++;
    }

}
