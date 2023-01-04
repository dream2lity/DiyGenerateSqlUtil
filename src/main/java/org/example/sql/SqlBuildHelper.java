package org.example.sql;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import org.example.sql.interpreter.OperatorInterpreter;
import org.springframework.util.DigestUtils;
import org.springframework.util.StringUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * {@link OperatorSqlAdapter} 和 {@link OperatorInterpreter} 的辅助
 * @author chijiuwang
 */
public class SqlBuildHelper {

    private static final String TABLE_PREFIX = "T_";
    public static final String SQL_WRAPPER = "`";
    public static final String PLACEHOLDER_PREFIX = "${";
    public static final String PLACEHOLDER_PREFIX_REGEX = "\\$\\{";
    public static final String PLACEHOLDER_SUFFIX = "}";
    public static final String PLACEHOLDER_SUFFIX_REGEX = "\\}";
    public static final String TABLE_PERMISSION_PLACEHOLDER_SUFFIX = "_permission";

    /**
     * 生成表名表达式，如 `table` `T_704BA3`
     * @param tableName 表名称
     * @param tableNameAlias 别名
     * @return 表名表达式
     */
    public static SQLExprTableSource tableNameWithAlias(String tableName, String tableNameAlias) {
        SQLExprTableSource tableSource = new SQLExprTableSource(new SQLIdentifierExpr(quote(tableName)));
        tableSource.setAlias(quote(tableNameAlias));
        return tableSource;
    }

    public static SQLSubqueryTableSource tableWithAlias(SQLSelectQueryBlock queryBlock, String tableNameAlias) {
        SQLSubqueryTableSource sqlSubqueryTableSource = new SQLSubqueryTableSource(queryBlock);
        sqlSubqueryTableSource.setAlias(quote(tableNameAlias));
        return sqlSubqueryTableSource;
    }

    public static SQLPropertyExpr fullField(String tableName, String fieldName) {
        return new SQLPropertyExpr(new SQLIdentifierExpr(quote(tableName)), quote(fieldName));
    }

    public static SQLIdentifierExpr simpleFunctionField(String tableName, String fieldName, String functionPrefix, String functionSuffix) {
        SQLPropertyExpr field = fullField(tableName, fieldName);
        return wrapFieldWithFunction(field, functionPrefix, functionSuffix);
    }

    public static SQLIdentifierExpr simpleFunctionField(String tableName, String fieldName, String functionFormat, int argCount) {
        SQLPropertyExpr field = fullField(tableName, fieldName);
        return wrapFieldWithFunction(field, functionFormat, argCount);
    }

    public static SQLIdentifierExpr wrapFieldWithFunction(SQLExpr expr, String functionPrefix, String functionSuffix) {
        String newExpr = functionPrefix + SQLUtils.toSQLString(expr) + functionSuffix;
        return new SQLIdentifierExpr(newExpr);
    }

    public static SQLIdentifierExpr wrapFieldWithFunction(SQLExpr expr, String functionFormat, int argCount) {
        String newExpr = String.format(functionFormat, (Object[]) Collections.nCopies(argCount, SQLUtils.toSQLString(expr)).toArray(new String[0]));
        return new SQLIdentifierExpr(newExpr);
    }

    public static SQLBinaryOpExpr joinCondition(SQLExpr left, SQLExpr right) {
        return new SQLBinaryOpExpr(left, SQLBinaryOperator.Equality, right);
    }

    public static SQLBinaryOpExpr multiJoinCondition(List<SQLBinaryOpExpr> conditions) {
        if (conditions.size() == 1) {
            return conditions.get(0);
        }
        SQLBinaryOpExpr left = conditions.get(0);
        for (int i = 1; i < conditions.size(); i++) {
            left = new SQLBinaryOpExpr(left, SQLBinaryOperator.BooleanAnd, conditions.get(i));
        }
        return left;
    }

    public static SQLBinaryOpExprGroup groupMultiCondition(SQLBinaryOperator operator, List<SQLExpr> conditions) {
        SQLBinaryOpExprGroup exprGroup = new SQLBinaryOpExprGroup(operator);
        conditions.stream().filter(Objects::nonNull).forEach(exprGroup::add);
        return exprGroup;
    }

    public static Map<SQLExpr, String> getCurrentColumnAlias(SQLSelectQueryBlock query, Set<SQLExpr> columns) {
        Deque<List<SQLSelectItem>> stack = new ArrayDeque<>();
        SQLTableSource from = query.getFrom();
        stack.addFirst(query.getSelectList());
        findAllSelectItems(stack, from);
        Map<SQLExpr, String> currAliasMap = new HashMap<>(columns.size());
        Map<String, SQLExpr> columnExpr = columns.stream().collect(Collectors.toMap(SQLUtils::toSQLString, c -> c, (o1, o2) -> o1));
        while (!stack.isEmpty()) {
            List<SQLSelectItem> sqlSelectItems = stack.removeFirst();
            for (SQLSelectItem i : sqlSelectItems) {
                String fullExprStr = SQLUtils.toSQLString(i);
                for (String s : columnExpr.keySet()) {
                    String currAlias = currAliasMap.get(columnExpr.get(s));
                    if (fullExprStr.contains(s)) {
                        currAliasMap.put(columnExpr.get(s), i.getAlias());
                    } else if (Objects.nonNull(currAlias) && fullExprStr.contains(currAlias)) {
                        currAliasMap.put(columnExpr.get(s), i.getAlias());
                    }
                }

            }
        }
        return currAliasMap;
    }

    public static Map<FuncPrefixCol, String> getCurrentColumnAliasWithFuncPrefix(SQLSelectQueryBlock query, Set<FuncPrefixCol> columns) {
        Deque<List<SQLSelectItem>> stack = new ArrayDeque<>();
        SQLTableSource from = query.getFrom();
        stack.addFirst(query.getSelectList());
        findAllSelectItems(stack, from);
        Map<FuncPrefixCol, String> currAliasMap = new HashMap<>(columns.size());
        Map<FuncPrefixCol, List<SQLSelectItem>> matches = new HashMap<>();
        while (!stack.isEmpty()) {
            List<SQLSelectItem> sqlSelectItems = stack.removeFirst();
            for (SQLSelectItem i : sqlSelectItems) {
                String fullExprStr = SQLUtils.toSQLString(i).toLowerCase();
                for (FuncPrefixCol funcPrefixCol : columns) {
                    String currAlias = currAliasMap.get(funcPrefixCol);
                    String functionPrefix = funcPrefixCol.getFunctionPrefix().toLowerCase();
                    String s = SQLUtils.toSQLString(funcPrefixCol.getExpr()).toLowerCase();
                    if (fullExprStr.contains(s)) {
                        currAliasMap.put(funcPrefixCol, i.getAlias());
                    } else if (Objects.nonNull(currAlias) && fullExprStr.contains(currAlias.toLowerCase())) {
                        currAliasMap.put(funcPrefixCol, i.getAlias());
                        if (stack.isEmpty() && StringUtils.isEmpty(functionPrefix)) {
                            if (!matches.containsKey(funcPrefixCol)) {
                                matches.put(funcPrefixCol, new ArrayList<>());
                            }
                            matches.get(funcPrefixCol).add(i);
                        }
                        currAliasMap.put(funcPrefixCol, i.getAlias());
                    }
                }
            }
        }
        if (!matches.isEmpty()) {
            for (FuncPrefixCol funcPrefixCol : matches.keySet()) {
                List<SQLSelectItem> sqlSelectItems = matches.get(funcPrefixCol);
                if (sqlSelectItems.size() > 1) {
                    sqlSelectItems.stream().filter(s -> {
                        String fullExprStr = SQLUtils.toSQLString(s).toLowerCase();
                        return fullExprStr.contains(funcPrefixCol.getFunctionPrefix().toLowerCase());
                    }).findFirst().ifPresent(sqlSelectItem -> currAliasMap.put(funcPrefixCol, sqlSelectItem.getAlias()));
                }
            }
        }
        return currAliasMap;
    }

    @AllArgsConstructor
    @Data
    public static class FuncPrefixCol {
        private SQLExpr expr;
        private String functionPrefix;
    }

    public static Map<SQLExpr, List<SQLSelectItem>> getCurrentProbablyColumnAlias(SQLSelectQueryBlock query, Set<SQLExpr> columns) {
        Deque<List<SQLSelectItem>> stack = new ArrayDeque<>();
        SQLTableSource from = query.getFrom();
        stack.addFirst(query.getSelectList());
        findAllSelectItems(stack, from);
        Map<SQLExpr, List<SQLSelectItem>> currAliasMap = new HashMap<>(columns.size());
        Map<String, SQLExpr> columnExpr = columns.stream().collect(Collectors.toMap(SQLUtils::toSQLString, c -> c, (o1, o2) -> o1));
        while (!stack.isEmpty()) {
            List<SQLSelectItem> sqlSelectItems = stack.removeFirst();
            for (SQLSelectItem i : sqlSelectItems) {
                String fullExprStr = SQLUtils.toSQLString(i);
                for (String s : columnExpr.keySet()) {
                    List<SQLSelectItem> probablyCurrAlias = currAliasMap.get(columnExpr.get(s));
                    for (SQLSelectItem currItem : probablyCurrAlias) {
                        if (fullExprStr.contains(s)) {
                            if (!currAliasMap.containsKey(columnExpr.get(s))) {
                                currAliasMap.put(columnExpr.get(s), new ArrayList<>());
                            }
                            currAliasMap.get(columnExpr.get(s)).add(i);
                        } else if (Objects.nonNull(currItem) && fullExprStr.contains(currItem.getAlias())) {
                            if (!currAliasMap.containsKey(columnExpr.get(s))) {
                                currAliasMap.put(columnExpr.get(s), new ArrayList<>());
                            }
                            currAliasMap.get(columnExpr.get(s)).add(i);
                        }
                    }
                }

            }
        }
        for (SQLExpr sqlExpr : currAliasMap.keySet()) {
            List<SQLSelectItem> currItems = currAliasMap.get(sqlExpr).stream().filter(i -> query.getSelectList().contains(i)).collect(Collectors.toList());
            currAliasMap.put(sqlExpr, currItems);
        }
        return currAliasMap;
    }

    public static String getCurrentColumnAlias(SQLSelectQueryBlock query, SQLExpr column) {
        Deque<List<SQLSelectItem>> stack = new ArrayDeque<>();
        SQLTableSource from = query.getFrom();
        stack.addFirst(query.getSelectList());
        findAllSelectItems(stack, from);
        String currAlias = null;
        String columnExprStr = SQLUtils.toSQLString(column);
        while (!stack.isEmpty()) {
            List<SQLSelectItem> sqlSelectItems = stack.removeFirst();
            for (SQLSelectItem i : sqlSelectItems) {
                String fullExprStr = SQLUtils.toSQLString(i);
                if (fullExprStr.contains(columnExprStr)) {
                    currAlias = i.getAlias();
                } else if (Objects.nonNull(currAlias) && fullExprStr.contains(currAlias)) {
                    currAlias = i.getAlias();
                }
            }
        }
        return currAlias;
    }

    public static String getCurrentColumnAliasWithFuncPrefix(SQLSelectQueryBlock query, FuncPrefixCol column) {
        Deque<List<SQLSelectItem>> stack = new ArrayDeque<>();
        SQLTableSource from = query.getFrom();
        stack.addFirst(query.getSelectList());
        findAllSelectItems(stack, from);
        String currAlias = null;
        String columnExprStr = SQLUtils.toSQLString(column.getExpr()).toLowerCase();
        String functionPrefix = column.getFunctionPrefix().toLowerCase();
        List<SQLSelectItem> matches = new ArrayList<>();
        while (!stack.isEmpty()) {
            List<SQLSelectItem> sqlSelectItems = stack.removeFirst();
            for (SQLSelectItem i : sqlSelectItems) {
                String fullExprStr = SQLUtils.toSQLString(i).toLowerCase();
                if (fullExprStr.contains(columnExprStr)) {
                    currAlias = i.getAlias();
                } else if (Objects.nonNull(currAlias) && fullExprStr.contains(currAlias.toLowerCase())) {
                    if (stack.isEmpty() && !StringUtils.isEmpty(functionPrefix)) {
                        matches.add(i);
                    }
                    currAlias = i.getAlias();
                }
            }
        }
        if (matches.size() > 1) {
            Optional<SQLSelectItem> match = matches.stream().filter(s -> {
                String fullExprStr = SQLUtils.toSQLString(s).toLowerCase();
                return fullExprStr.contains(functionPrefix);
            }).findFirst();
            if (match.isPresent()) {
                return match.get().getAlias();
            }
        }
        return currAlias;
    }

    /**
     * 查找字段在当前查询中的别名
     * @param query 查询SQL
     * @param column 要查找的字段
     * @param allFields 查询中可能用到的字段列表
     * @return 字段别名
     */
    public static String getCurrentColumnAliasWithFuncPrefix(SQLSelectQueryBlock query, OperatorSqlAdapter.FieldInfo column, Map<String, OperatorSqlAdapter.FieldInfo> allFields) {
        Deque<List<SQLSelectItem>> stack = new ArrayDeque<>();
        SQLTableSource from = query.getFrom();
        stack.addFirst(query.getSelectList());
        findAllSelectItems(stack, from);
        Set<String> currAliasSet = new HashSet<>();

        String columnExprStr = column.getFieldExpr().toLowerCase();
        String functionPrefix = column.getFunctionPrefix().toLowerCase();

        Map<String, SQLSelectItem> aliasMatches = new HashMap<>();

        Map<Integer, List<SQLSelectItem>> levelAliasMatches = new HashMap<>();
        int level = 0;
        while (!stack.isEmpty()) {
            List<SQLSelectItem> sqlSelectItems = stack.removeFirst();
            for (SQLSelectItem i : sqlSelectItems) {
                fillAliasMatches(currAliasSet, aliasMatches, levelAliasMatches, level, i, columnExprStr);
            }
            level++;
        }
        if (levelAliasMatches.get(level - 1).size() == 1) {
            return levelAliasMatches.get(level - 1).get(0).getAlias();
        }
        functionPrefix = getFirstFunction(allFields, column, functionPrefix);
        if (StringUtils.hasText(functionPrefix)) {
            for (int i = level - 1; i >= 0; i--) {
                List<SQLSelectItem> items = levelAliasMatches.get(i);
                if (Objects.isNull(items)) {
                    continue;
                }
                for (SQLSelectItem item : items) {
                    String fullExprStr = SQLUtils.toSQLString(item).toLowerCase();
                    String prefix = fullExprStr.substring(0, fullExprStr.indexOf(SQL_WRAPPER));
                    if (functionPrefix.equals(prefix)) {
                        if (i == level - 1) {
                            return item.getAlias();
                        } else {
                            SQLSelectItem selectItem = aliasMatches.get(item.getAlias());
                            while (aliasMatches.containsKey(selectItem.getAlias())) {
                                selectItem = aliasMatches.get(selectItem.getAlias());
                            }
                            return selectItem.getAlias();
                        }
                    }
                }
            }
        }

        return null;
    }

    /**
     * 批量查找字段在当前查询中的别名
     * @param query 查询SQL
     * @param columns 要查找的字段
     * @param allFields 查询中可能用到的字段列表
     * @return 字段和字段别名的映射
     */
    public static Map<OperatorSqlAdapter.FieldInfo, String> getCurrentColumnAliasWithFuncPrefix(SQLSelectQueryBlock query, Set<OperatorSqlAdapter.FieldInfo> columns, Map<String, OperatorSqlAdapter.FieldInfo> allFields) {
        Deque<List<SQLSelectItem>> stack = new ArrayDeque<>();
        SQLTableSource from = query.getFrom();
        stack.addFirst(query.getSelectList());
        findAllSelectItems(stack, from);
        Map<OperatorSqlAdapter.FieldInfo, Set<String>> currAliasSet = new HashMap<>();

        Map<OperatorSqlAdapter.FieldInfo, Map<String, SQLSelectItem>> aliasMatches = new HashMap<>();

        Map<OperatorSqlAdapter.FieldInfo, Map<Integer, List<SQLSelectItem>>> levelAliasMatches = new HashMap<>();

        int level = 0;
        while (!stack.isEmpty()) {
            List<SQLSelectItem> sqlSelectItems = stack.removeFirst();
            for (SQLSelectItem i : sqlSelectItems) {
                for (OperatorSqlAdapter.FieldInfo column : columns) {
                    String columnExprStr = column.getFieldExpr().toLowerCase();
                    if (!currAliasSet.containsKey(column)) {
                        currAliasSet.put(column, new HashSet<>());
                    }
                    if (!aliasMatches.containsKey(column)) {
                        aliasMatches.put(column, new HashMap<>());
                    }
                    if (!levelAliasMatches.containsKey(column)) {
                        levelAliasMatches.put(column, new HashMap<>());
                    }
                    fillAliasMatches(currAliasSet.get(column), aliasMatches.get(column), levelAliasMatches.get(column), level, i, columnExprStr);
                }

            }
            level++;
        }

        Map<OperatorSqlAdapter.FieldInfo, String> currAliasMap = new HashMap<>(columns.size());
        for (OperatorSqlAdapter.FieldInfo column : columns) {
            if (levelAliasMatches.get(column).get(level - 1).size() == 1) {
                currAliasMap.put(column, levelAliasMatches.get(column).get(level - 1).get(0).getAlias());
                continue;
            }
            String functionPrefix = column.getFunctionPrefix().toLowerCase();
            functionPrefix = getFirstFunction(allFields, column, functionPrefix);
            if (StringUtils.hasText(functionPrefix)) {
                for (int i = level - 1; i >= 0; i--) {
                    if (currAliasMap.containsKey(column)) {
                        break;
                    }
                    List<SQLSelectItem> items = levelAliasMatches.get(column).get(i);
                    if (Objects.isNull(items)) {
                        continue;
                    }
                    for (SQLSelectItem item : items) {
                        String fullExprStr = SQLUtils.toSQLString(item).toLowerCase();
                        String prefix = fullExprStr.substring(0, fullExprStr.indexOf(SQL_WRAPPER));
                        if (functionPrefix.equals(prefix) || fullExprStr.contains(column.getFieldExpr().toLowerCase())) {
                            if (i == level - 1) {
                                currAliasMap.put(column, item.getAlias());
                            } else {
                                SQLSelectItem selectItem = aliasMatches.get(column).get(item.getAlias());
                                while (aliasMatches.get(column).containsKey(selectItem.getAlias())) {
                                    selectItem = aliasMatches.get(column).get(selectItem.getAlias());
                                }
                                currAliasMap.put(column, selectItem.getAlias());
                            }
                            break;
                        }
                    }
                }
            }
        }

        return currAliasMap;
    }

    private static String getFirstFunction(Map<String, OperatorSqlAdapter.FieldInfo> allFields, OperatorSqlAdapter.FieldInfo column, String functionPrefix) {
        if (!StringUtils.hasText(functionPrefix)) {
            OperatorSqlAdapter.FieldInfo parent = column;
            while (!StringUtils.hasText(parent.getFunctionPrefix()) && (parent.getSourceFieldId() == null || !parent.getSourceFieldId().equals(parent.getFieldId()))) {
                if (allFields.get(parent.getSourceFieldId()) == null) {
                    break;
                }
                parent = allFields.get(parent.getSourceFieldId());
            }
            functionPrefix = parent.getFunctionPrefix().toLowerCase();
        }
        return functionPrefix;
    }

    private static void fillAliasMatches(Set<String> currAliasSet, Map<String, SQLSelectItem> aliasMatches, Map<Integer, List<SQLSelectItem>> levelAliasMatches, int level, SQLSelectItem i, String columnExprStr) {
        String fullExprStr = SQLUtils.toSQLString(i).toLowerCase();
        if (fullExprStr.contains(columnExprStr) || aliasSetContains(fullExprStr, currAliasSet)) {
            currAliasSet.add(i.getAlias());

            aliasMatches.put(fullExprStr.contains(columnExprStr) ? columnExprStr : aliasSetMatch(i, currAliasSet), i);
            if (!levelAliasMatches.containsKey(level)) {
                levelAliasMatches.put(level, new ArrayList<>());
            }
            levelAliasMatches.get(level).add(i);
        }
    }

    private static String aliasSetMatch(SQLSelectItem i, Set<String> currAliasSet) {
        String expr = SQLUtils.toSQLString(i.getExpr()).toLowerCase();
        return currAliasSet.stream().filter(a -> expr.split("as")[0].contains(a.toLowerCase())).findFirst().orElse("");
    }

    private static boolean aliasSetContains(String fullExprStr, Set<String> currAliasSet) {
        return !currAliasSet.isEmpty() && currAliasSet.stream().anyMatch(a -> fullExprStr.contains(a.toLowerCase()));
    }

    public static List<SQLSelectItem> getCurrentProbablyColumnAlias(SQLSelectQueryBlock query, SQLExpr column) {
        Deque<List<SQLSelectItem>> stack = new ArrayDeque<>();
        SQLTableSource from = query.getFrom();
        stack.addFirst(query.getSelectList());
        findAllSelectItems(stack, from);
        String currAlias = null;
        List<SQLSelectItem> historyItems = new ArrayList<>();
        String columnExprStr = SQLUtils.toSQLString(column);
        while (!stack.isEmpty()) {
            List<SQLSelectItem> sqlSelectItems = stack.removeFirst();
            for (SQLSelectItem i : sqlSelectItems) {
                String fullExprStr = SQLUtils.toSQLString(i);
                if (fullExprStr.contains(columnExprStr)) {
                    currAlias = i.getAlias();
                    historyItems.add(i);
                } else if (Objects.nonNull(currAlias) && fullExprStr.contains(currAlias)) {
                    currAlias = i.getAlias();
                    historyItems.add(i);
                }
            }
        }

        return historyItems.stream().filter(i -> query.getSelectList().contains(i)).collect(Collectors.toList());
    }

    /**
     * 找出所有查询对象，包括子查询中的查询，放到stack中 <br/>
     * union类型的数据源还没有经过测试 alpha
     * @param stack 存储栈
     * @param tableSource 数据源
     */
    private static void findAllSelectItems(Deque<List<SQLSelectItem>> stack, SQLTableSource tableSource) {
        if (tableSource instanceof SQLSubqueryTableSource) {
            SQLSubqueryTableSource subQueryTableSource = (SQLSubqueryTableSource) tableSource;
            SQLSelectQueryBlock subQuery = (SQLSelectQueryBlock) subQueryTableSource.getSelect().getQuery();
            stack.addFirst(subQuery.getSelectList());
            findAllSelectItems(stack, subQuery.getFrom());
        } else if (tableSource instanceof SQLJoinTableSource) {
            SQLJoinTableSource sqlJoinTableSource = (SQLJoinTableSource) tableSource;
            findAllSelectItems(stack, sqlJoinTableSource.getLeft());
            findAllSelectItems(stack, sqlJoinTableSource.getRight());
        } else if (tableSource instanceof SQLUnionQueryTableSource) {
            SQLUnionQueryTableSource sqlUnionQueryTableSource = (SQLUnionQueryTableSource) tableSource;
            for (SQLSelectQuery child : sqlUnionQueryTableSource.getUnion().getChildren()) {
                SQLSelectQueryBlock query = (SQLSelectQueryBlock) child;
                stack.addFirst(query.getSelectList());
                findAllSelectItems(stack, query.getFrom());
            }
        }
    }

    public static SQLSelectGroupByClause generateGroupBy(int nums) {
        SQLSelectGroupByClause sqlSelectGroupByClause = new SQLSelectGroupByClause();
        IntStream.rangeClosed(1, nums).forEach(i -> sqlSelectGroupByClause.addItem(new SQLIdentifierExpr(String.valueOf(i))));
        return sqlSelectGroupByClause;
    }

    public static String quote(String s) {
        return SQL_WRAPPER + s + SQL_WRAPPER;
    }

    public static String unQuote(String s) {
        if (Objects.isNull(s)) {
            return null;
        }
        if (s.startsWith(SQL_WRAPPER)) {
            s = s.substring(1, s.length() - 1);
        }
        if (s.endsWith(SQL_WRAPPER)) {
            s = s.substring(0, s.length() - 2);
        }
        return s;
    }

    /**
     * 生成内部表名
     * @param tableName 表名
     * @return 内部表名
     */
    public static String generateTableName(String tableName) {
        return TABLE_PREFIX + DigestUtils.md5DigestAsHex(tableName.getBytes()).substring(0, 6).toUpperCase();
    }

    public static String generatePlaceholder(String placeHolderName) {
        return PLACEHOLDER_PREFIX + placeHolderName + PLACEHOLDER_SUFFIX;
    }

    public static String generateRegexPlaceholder(String placeHolderName) {
        return PLACEHOLDER_PREFIX_REGEX + placeHolderName + PLACEHOLDER_SUFFIX_REGEX;
    }

    public static String generateTablePermissionPlaceHolder(String tableName) {
        return generatePlaceholder(generateTableName(tableName) + TABLE_PERMISSION_PLACEHOLDER_SUFFIX);
    }

    public static String generateInnerTablePermissionPlaceHolderName(String innerTableName) {
        return innerTableName + TABLE_PERMISSION_PLACEHOLDER_SUFFIX;
    }

}
