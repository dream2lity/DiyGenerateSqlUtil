package org.example.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.sql.base.OperatorCheckResult;
import org.example.sql.base.OperatorField;
import org.example.sql.base.PlaceHolderInfo;
import org.example.sql.base.RelationTableInfo;
import org.example.sql.operator.AnalysisOperator;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

/**
 * @author chijiuwang
 */
@Slf4j
public class OperatorSqlProxy {

    protected OperatorSqlInterpreter operatorSqlInterpreter;
    protected boolean isSetFields;
    protected boolean isBuildSql;
    protected boolean isCheck;

    public OperatorSqlProxy(List<AnalysisOperator<?>> operators, ObjectMapper objectMapper) {
        this.operatorSqlInterpreter = new OperatorSqlInterpreter(objectMapper, operators);
        this.isSetFields = false;
        this.isBuildSql = false;
        this.isCheck = false;
    }

    public OperatorSqlProxy(List<AnalysisOperator<?>> operators, ObjectMapper objectMapper, Function<Long, List<AnalysisOperator<?>>> getTableOperators) {
        this.operatorSqlInterpreter = new OperatorSqlInterpreter(objectMapper, operators);
        this.operatorSqlInterpreter.setGetTableOperators(getTableOperators);
        this.isSetFields = false;
        this.isBuildSql = false;
        this.isCheck = false;
    }

    public void setBuildSqlWithPlaceHolder(boolean buildSqlWithPlaceHolder) {
        this.operatorSqlInterpreter.setBuildWithPlaceHolder(buildSqlWithPlaceHolder);
    }

    public void setTableRowCount(int tableRowCount) {
        this.operatorSqlInterpreter.setTableRowCount(tableRowCount);
    }

    /**
     * 设置最终的分页参数
     * @param offset 偏移量
     * @param rowCount 行数
     */
    public void setOffset(int offset, int rowCount) {
        this.operatorSqlInterpreter.setOffset(offset, rowCount);
    }

    public void reset() {
        OperatorSqlInterpreter operatorSqlInterpreter = new OperatorSqlInterpreter(this.operatorSqlInterpreter.getObjectMapper(), this.operatorSqlInterpreter.getOperators());
        operatorSqlInterpreter.setGetTableOperators(this.operatorSqlInterpreter.getGetTableOperators());
        this.operatorSqlInterpreter = operatorSqlInterpreter;
        this.isSetFields = false;
        this.isBuildSql = false;
    }

    public List<OperatorCheckResult> check() {
        if (!this.isCheck) {
            this.operatorSqlInterpreter.checkOperators();
            this.isCheck = true;
        }
        return this.operatorSqlInterpreter.getOperatorCheckResults();
    }

    public List<OperatorField> getFields() {
        if (this.isSetFields && !this.isBuildSql) {
            return this.operatorSqlInterpreter.getFields();
        }
        if (this.isSetFields || this.isBuildSql) {
            this.operatorSqlInterpreter.clearFields();
        }
        long startTime = System.currentTimeMillis();
        this.operatorSqlInterpreter.generateFieldList();
        log.info("generate field List with time[{}ms]", System.currentTimeMillis() - startTime);
        this.isSetFields = true;
        return this.operatorSqlInterpreter.getFields();
    }

    public List<OperatorField> getFieldsWithBuildSql() {
        if (!this.isBuildSql) {
            buildSql();
        }
        return this.operatorSqlInterpreter.getFields();
    }

    public String getBuildSql() {
        if (!this.isBuildSql) {
            buildSql();
        }
        return this.operatorSqlInterpreter.getSql();
    }

    private void buildSql() {
        if (this.isSetFields || this.isBuildSql) {
            this.operatorSqlInterpreter.clearFields();
        }
        long startTime = System.currentTimeMillis();
        this.operatorSqlInterpreter.buildSql();
        log.info("build sql with time[{}ms]", System.currentTimeMillis() - startTime);
        this.isSetFields = true;
        this.isBuildSql = true;
    }

    public List<PlaceHolderInfo> getPlaceHolders() {
        if (!this.isBuildSql) {
            buildSql();
        }
        return this.operatorSqlInterpreter.getPlaceHolderInfos();
    }

    public Set<RelationTableInfo> getRelationTables() {
        if (!this.isBuildSql) {
            buildSql();
        }
        return this.operatorSqlInterpreter.getRelatedTables();
    }
}
