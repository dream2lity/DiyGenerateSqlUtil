package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.sql.OperatorSqlProxy;
import org.example.sql.TableTransferOperatorSqlProxy;
import org.example.sql.operator.AnalysisOperator;
import org.example.sql.operator.ObjectMapperReflectionTypeRegister;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author chijiuwang
 */
public class OperatorSqlProxyTests {

    ObjectMapper objectMapper = new ObjectMapper();

    Map<Long, String> getTableOperator;

    @Before
    public void init() {
        new ObjectMapperReflectionTypeRegister(objectMapper);
        getTableOperator = new HashMap<>();
        getTableOperator.put(11L, "[{\"type\":\"select_field\",\"customName\":\"选字段\",\"value\":{\"tableType\":1,\"tableName\":\"employee\",\"fields\":[{\"fieldName\":\"id\",\"fieldType\":1},{\"fieldName\":\"name\",\"fieldType\":0},{\"fieldName\":\"dept_id\",\"fieldType\":1},{\"fieldName\":\"group_id\",\"fieldType\":1},{\"fieldName\":\"create_time\",\"fieldType\":2},{\"fieldName\":\"age\",\"fieldType\":1},{\"fieldName\":\"score\",\"fieldType\":0}]}},{\"type\":\"join\",\"customName\":\"左右合并\",\"value\":{\"style\":1,\"basis\":[[\"dept_id\",\"id\"]],\"table\":{\"tableType\":1,\"tableName\":\"department\",\"fields\":[{\"fieldName\":\"id\",\"fieldType\":1},{\"fieldName\":\"name\",\"fieldType\":0}]}}},{\"type\":\"join\",\"customName\":\"左右合并\",\"value\":{\"style\":3,\"basis\":[[\"group_id\",\"id\"]],\"table\":{\"tableType\":1,\"tableName\":\"class_info\",\"fields\":[{\"fieldName\":\"id\",\"fieldType\":1},{\"fieldName\":\"class_name\",\"fieldType\":0}]}}},{\"type\":\"set_field\",\"customName\":\"字段设置\",\"value\":{\"fieldList\":[{\"fieldId\":\"id\",\"transferName\":\"id\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"name\",\"transferName\":\"name\",\"fieldType\":0,\"used\":true},{\"fieldId\":\"dept_id\",\"transferName\":\"dept_id\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"group_id\",\"transferName\":\"group_id\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"create_time\",\"transferName\":\"create_time\",\"fieldType\":2,\"used\":true},{\"fieldId\":\"age\",\"transferName\":\"age\",\"fieldType\":1,\"used\":false},{\"fieldId\":\"score\",\"transferName\":\"score\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"id1\",\"transferName\":\"id\",\"fieldType\":1,\"used\":false},{\"fieldId\":\"name1\",\"transferName\":\"dept_name\",\"fieldType\":0,\"used\":true},{\"fieldId\":\"id2\",\"transferName\":\"id\",\"fieldType\":1,\"used\":false},{\"fieldId\":\"class_name\",\"transferName\":\"class_name\",\"fieldType\":0,\"used\":true}]}},{\"type\":\"group_by\",\"customName\":\"分组汇总\",\"value\":{\"dimensions\":[{\"fieldId\":\"create_time\",\"transferName\":\"create_time\",\"fieldType\":2,\"functionType\":1},{\"fieldId\":\"dept_name\",\"transferName\":\"dept_name\",\"fieldType\":0},{\"fieldId\":\"class_name\",\"transferName\":\"class_name\",\"fieldType\":0}],\"norms\":[{\"fieldId\":\"name\",\"transferName\":\"name\",\"fieldType\":0,\"functionType\":1},{\"fieldId\":\"name\",\"transferName\":\"name\",\"fieldType\":0,\"functionType\":2},{\"fieldId\":\"score\",\"transferName\":\"score\",\"fieldType\":1,\"functionType\":1},{\"fieldId\":\"score\",\"transferName\":\"score\",\"fieldType\":1,\"functionType\":2},{\"fieldId\":\"score\",\"transferName\":\"score\",\"fieldType\":1,\"functionType\":3},{\"fieldId\":\"score\",\"transferName\":\"score的最小值\",\"fieldType\":1,\"functionType\":4}]}},{\"type\":\"filter\",\"customName\":\"过滤\",\"value\":{\"filterType\":1,\"children\":[{\"filterValue\":[{\"value\":\"销售部\"},{\"value\":\"运营部\"}],\"fieldId\":\"dept_name\",\"filterType\":13},{\"filterValue\":1,\"fieldId\":\"name\",\"filterType\":7},{\"filterValue\":90,\"fieldId\":\"score1\",\"filterType\":7},{\"filterValue\":{\"min\":100.6,\"max\":190},\"fieldId\":\"score\",\"filterType\":3},{\"filterValue\":{\"date\":\"2022-01-21\"},\"fieldId\":\"create_time\",\"filterType\":25}]}}]");
        getTableOperator.put(12L, "[{\"type\":\"select_field\",\"customName\":\"选字段\",\"value\":{\"tableType\":1,\"tableId\":25,\"tableName\":\"table_a\",\"fields\":[{\"fieldName\":\"a_id\",\"fieldType\":1},{\"fieldName\":\"a_name\",\"fieldType\":0},{\"fieldName\":\"a_score\",\"fieldType\":1},{\"fieldName\":\"a_age\",\"fieldType\":1},{\"fieldName\":\"a_class\",\"fieldType\":0},{\"fieldName\":\"a_grade\",\"fieldType\":0}]}},{\"type\":\"group_by\",\"customName\":\"分组汇总\",\"value\":{\"dimensions\":[{\"fieldId\":\"a_class\",\"transferName\":\"a_class\",\"fieldType\":0},{\"fieldId\":\"a_grade\",\"transferName\":\"a_grade\",\"fieldType\":0}],\"norms\":[{\"fieldId\":\"a_name\",\"transferName\":\"a_name\",\"fieldType\":0,\"functionType\":1},{\"fieldId\":\"a_name\",\"transferName\":\"a_name\",\"fieldType\":0,\"functionType\":2},{\"fieldId\":\"a_score\",\"transferName\":\"a_score\",\"fieldType\":1,\"functionType\":1},{\"fieldId\":\"a_score\",\"transferName\":\"a_score\",\"fieldType\":1,\"functionType\":2},{\"fieldId\":\"a_score\",\"transferName\":\"a_score\",\"fieldType\":1,\"functionType\":3},{\"fieldId\":\"a_score\",\"transferName\":\"a_score的最小值\",\"fieldType\":1,\"functionType\":4}]}}]");
        getTableOperator.put(13L, "[{\"type\":\"select_field\",\"customName\":\"选字段\",\"value\":{\"tableType\":1,\"tableId\":30,\"tableName\":\"table_b\",\"fields\":[{\"fieldName\":\"b_id\",\"fieldType\":1},{\"fieldName\":\"b_name\",\"fieldType\":0},{\"fieldName\":\"b_score\",\"fieldType\":1},{\"fieldName\":\"b_age\",\"fieldType\":1},{\"fieldName\":\"b_class\",\"fieldType\":0},{\"fieldName\":\"b_grade\",\"fieldType\":0}]}},{\"type\":\"group_by\",\"customName\":\"分组汇总\",\"value\":{\"dimensions\":[{\"fieldId\":\"b_class\",\"transferName\":\"b_class\",\"fieldType\":0},{\"fieldId\":\"b_grade\",\"transferName\":\"b_grade\",\"fieldType\":0}],\"norms\":[{\"fieldId\":\"b_name\",\"transferName\":\"b_name\",\"fieldType\":0,\"functionType\":1},{\"fieldId\":\"b_name\",\"transferName\":\"b_name\",\"fieldType\":0,\"functionType\":2},{\"fieldId\":\"b_score\",\"transferName\":\"b_score\",\"fieldType\":1,\"functionType\":1},{\"fieldId\":\"b_score\",\"transferName\":\"b_score\",\"fieldType\":1,\"functionType\":2},{\"fieldId\":\"b_score\",\"transferName\":\"b_score\",\"fieldType\":1,\"functionType\":3},{\"fieldId\":\"b_score\",\"transferName\":\"b_score的最小值\",\"fieldType\":1,\"functionType\":4}]}}]");
        getTableOperator.put(14L, "[{\"type\":\"select_field\",\"customName\":\"选字段\",\"value\":{\"tableType\":1,\"tableId\":32,\"tableName\":\"table_c\",\"fields\":[{\"fieldName\":\"c_id\",\"fieldType\":1},{\"fieldName\":\"c_name\",\"fieldType\":0},{\"fieldName\":\"c_score\",\"fieldType\":1},{\"fieldName\":\"c_age\",\"fieldType\":1},{\"fieldName\":\"c_class\",\"fieldType\":0},{\"fieldName\":\"c_grade\",\"fieldType\":0}]}},{\"type\":\"group_by\",\"customName\":\"分组汇总\",\"value\":{\"dimensions\":[{\"fieldId\":\"c_class\",\"transferName\":\"c_class\",\"fieldType\":0},{\"fieldId\":\"c_grade\",\"transferName\":\"c_grade\",\"fieldType\":0}],\"norms\":[{\"fieldId\":\"c_name\",\"transferName\":\"c_name\",\"fieldType\":0,\"functionType\":1},{\"fieldId\":\"c_name\",\"transferName\":\"c_name\",\"fieldType\":0,\"functionType\":2},{\"fieldId\":\"c_score\",\"transferName\":\"c_score\",\"fieldType\":1,\"functionType\":1},{\"fieldId\":\"c_score\",\"transferName\":\"c_score\",\"fieldType\":1,\"functionType\":2},{\"fieldId\":\"c_score\",\"transferName\":\"c_score\",\"fieldType\":1,\"functionType\":3},{\"fieldId\":\"c_score\",\"transferName\":\"c_score的最小值\",\"fieldType\":1,\"functionType\":4}]}}]");
        getTableOperator.put(15L, "[{\"type\":\"select_field\",\"customName\":\"选字段\",\"value\":{\"tableType\":4,\"tableId\":12,\"tableName\":\"自助数据集1\",\"fields\":[{\"fieldName\":\"a_class\",\"fieldType\":0},{\"fieldName\":\"a_grade\",\"fieldType\":0},{\"fieldName\":\"a_name\",\"fieldType\":1},{\"fieldName\":\"a_name1\",\"fieldType\":1},{\"fieldName\":\"a_score\",\"fieldType\":1},{\"fieldName\":\"a_score2\",\"fieldType\":1},{\"fieldName\":\"a_score的最小值\",\"fieldType\":1}]}},{\"type\":\"join\",\"customName\":\"左右合并\",\"value\":{\"style\":1,\"basis\":[[\"a_class\",\"b_class\"],[\"a_grade\",\"b_grade\"]],\"table\":{\"tableType\":4,\"tableId\":13,\"tableName\":\"自助数据集2\",\"fields\":[{\"fieldName\":\"b_class\",\"fieldType\":0},{\"fieldName\":\"b_grade\",\"fieldType\":0},{\"fieldName\":\"b_name\",\"fieldType\":1},{\"fieldName\":\"b_name1\",\"fieldType\":1},{\"fieldName\":\"b_score\",\"fieldType\":1},{\"fieldName\":\"b_score1\",\"fieldType\":1},{\"fieldName\":\"b_score2\",\"fieldType\":1},{\"fieldName\":\"b_score的最小值\",\"fieldType\":1}]}}},{\"type\":\"join\",\"customName\":\"左右合并\",\"value\":{\"style\":1,\"basis\":[[\"a_class\",\"c_class\"],[\"a_grade\",\"c_grade\"]],\"table\":{\"tableType\":4,\"tableId\":14,\"tableName\":\"自助数据集3\",\"fields\":[{\"fieldName\":\"c_class\",\"fieldType\":0},{\"fieldName\":\"c_grade\",\"fieldType\":0},{\"fieldName\":\"c_name\",\"fieldType\":1},{\"fieldName\":\"c_name1\",\"fieldType\":1},{\"fieldName\":\"c_score\",\"fieldType\":1},{\"fieldName\":\"c_score1\",\"fieldType\":1},{\"fieldName\":\"c_score2\",\"fieldType\":1},{\"fieldName\":\"c_score的最小值\",\"fieldType\":1}]}}},{\"type\":\"set_field\",\"customName\":\"字段设置\",\"value\":{\"fieldList\":[{\"fieldId\":\"a_class\",\"transferName\":\"class\",\"fieldType\":0,\"used\":true},{\"fieldId\":\"a_grade\",\"transferName\":\"grade\",\"fieldType\":0,\"used\":true},{\"fieldId\":\"a_name\",\"transferName\":\"a_name 的总数\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"a_name1\",\"transferName\":\"a_name 去重计数\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"a_score\",\"transferName\":\"a_score 总数\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"a_score2\",\"transferName\":\"a_score 最大值\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"a_score的最小值\",\"transferName\":\"a_score的最小值\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"b_class\",\"transferName\":\"b_class\",\"fieldType\":0,\"used\":false},{\"fieldId\":\"b_grade\",\"transferName\":\"b_grade\",\"fieldType\":0,\"used\":false},{\"fieldId\":\"b_name\",\"transferName\":\"b_name 的总数\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"b_name1\",\"transferName\":\"b_name 去重计数\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"b_score\",\"transferName\":\"b_score 总数\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"b_score1\",\"transferName\":\"b_score 平均值\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"b_score2\",\"transferName\":\"b_score 最大值\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"b_score的最小值\",\"transferName\":\"b_score 最小值\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"c_class\",\"transferName\":\"c_class\",\"fieldType\":0,\"used\":false},{\"fieldId\":\"c_grade\",\"transferName\":\"c_grade\",\"fieldType\":0,\"used\":false},{\"fieldId\":\"c_name\",\"transferName\":\"c_name 总数\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"c_name1\",\"transferName\":\"c_name 去重计数\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"c_score\",\"transferName\":\"c_score 总数\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"c_score1\",\"transferName\":\"c_score 平均值\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"c_score2\",\"transferName\":\"c_score 最大值\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"c_score的最小值\",\"transferName\":\"c_score 最小值\",\"fieldType\":1,\"used\":true}]}},{\"type\":\"filter\",\"customName\":\"过滤\",\"value\":{\"filterType\":2,\"children\":[{\"children\":[{\"filterValue\":[{\"value\":\"foo_c\"},{\"value\":\"bar_c\"}],\"fieldId\":\"class\",\"filterType\":13},{\"filterValue\":13,\"fieldId\":\"a_score 总数\",\"filterType\":7},{\"filterValue\":{\"min\":23,\"max\":24},\"fieldId\":\"a_score 最大值\",\"filterType\":3}],\"filterType\":1},{\"filterValue\":\"bar\",\"fieldId\":\"grade\",\"filterType\":15}]}},{\"type\":\"group_by\",\"customName\":\"分组汇总\",\"value\":{\"dimensions\":[{\"fieldId\":\"a_name 的总数\",\"transferName\":\"a_name 的总数-新group\",\"fieldType\":1}],\"norms\":[{\"fieldId\":\"a_score的最小值\",\"transferName\":\"a_score的最小值\",\"fieldType\":1,\"functionType\":4},{\"fieldId\":\"b_score 最小值\",\"transferName\":\"b_score 最小值\",\"fieldType\":1,\"functionType\":4},{\"fieldId\":\"c_score 最小值\",\"transferName\":\"c_score的最小值\",\"fieldType\":1,\"functionType\":4}]}}]");
        getTableOperator.put(36L, "[{\"type\":\"select_field\",\"customName\":\"选字段\",\"value\":{\"tableType\":1,\"tableId\":33,\"tableName\":\"depart_agg\",\"fields\":[{\"fieldName\":\"id\",\"fieldType\":1},{\"fieldName\":\"depart\",\"fieldType\":0},{\"fieldName\":\"date\",\"fieldType\":2},{\"fieldName\":\"date_str\",\"fieldType\":0},{\"fieldName\":\"amount\",\"fieldType\":1},{\"fieldName\":\"income\",\"fieldType\":1}]}},{\"type\":\"filter\",\"customName\":\"过滤\",\"value\":{\"fieldId\":\"date\",\"filterType\":31,\"filterValue\":\"date\"}},{\"type\":\"group_by\",\"customName\":\"分组汇总\",\"value\":{\"dimensions\":[{\"fieldId\":\"depart\",\"transferName\":\"depart\",\"fieldType\":0}],\"norms\":[{\"fieldId\":\"amount\",\"transferName\":\"amount\",\"fieldType\":1,\"functionType\":1},{\"fieldId\":\"income\",\"transferName\":\"income\",\"fieldType\":1,\"functionType\":1}]}}]");
        getTableOperator.put(37L, "[{\"type\":\"select_field\",\"customName\":\"选字段\",\"value\":{\"tableType\":1,\"tableId\":34,\"tableName\":\"depart_raw\",\"fields\":[{\"fieldName\":\"id\",\"fieldType\":1},{\"fieldName\":\"depart\",\"fieldType\":0},{\"fieldName\":\"date\",\"fieldType\":2},{\"fieldName\":\"date_str\",\"fieldType\":0},{\"fieldName\":\"user\",\"fieldType\":0}]}},{\"type\":\"filter\",\"customName\":\"过滤\",\"value\":{\"fieldId\":\"date\",\"filterType\":31,\"filterValue\":\"date\"}},{\"type\":\"group_by\",\"customName\":\"分组汇总\",\"value\":{\"dimensions\":[{\"fieldId\":\"depart\",\"transferName\":\"depart\",\"fieldType\":0}],\"norms\":[{\"fieldId\":\"user\",\"transferName\":\"user_count\",\"fieldType\":1,\"functionType\":6}]}}]");
        getTableOperator.put(38L, "[{\"type\":\"select_field\",\"customName\":\"选字段\",\"value\":{\"tableType\":4,\"tableId\":36,\"tableName\":\"透传过滤器示例-depart_agg\",\"fields\":[{\"fieldName\":\"depart\",\"fieldType\":0},{\"fieldName\":\"amount\",\"fieldType\":1},{\"fieldName\":\"income\",\"fieldType\":1}]}},{\"type\":\"join\",\"customName\":\"左右合并\",\"value\":{\"style\":2,\"basis\":[[\"depart\",\"depart\"]],\"table\":{\"tableType\":4,\"tableId\":37,\"tableName\":\"透传过滤器示例-depart_raw\",\"fields\":[{\"fieldName\":\"depart\",\"fieldType\":0},{\"fieldName\":\"user_count\",\"fieldType\":1}]}}}]");
        getTableOperator.put(1L, "[{\"type\":\"select_field\",\"customName\":\"选字段\",\"value\":{\"tableType\":4,\"tableId\":12,\"tableName\":\"自助数据集1\",\"fields\":[{\"fieldName\":\"a_class\",\"fieldType\":0},{\"fieldName\":\"a_grade\",\"fieldType\":0},{\"fieldName\":\"a_name\",\"fieldType\":1},{\"fieldName\":\"a_name1\",\"fieldType\":1},{\"fieldName\":\"a_score\",\"fieldType\":1},{\"fieldName\":\"a_score2\",\"fieldType\":1},{\"fieldName\":\"a_score的最小值\",\"fieldType\":1}]}},{\"type\":\"join\",\"customName\":\"左右合并\",\"value\":{\"style\":1,\"basis\":[[\"a_class\",\"b_class\"],[\"a_grade\",\"b_grade\"]],\"table\":{\"tableType\":4,\"tableId\":13,\"tableName\":\"自助数据集2\",\"fields\":[]}}},{\"type\":\"join\",\"customName\":\"左右合并\",\"value\":{\"style\":1,\"basis\":[[\"a_class\",\"c_class\"],[\"a_grade\",\"c_grade\"]],\"table\":{\"tableType\":4,\"tableId\":14,\"tableName\":\"自助数据集3\",\"fields\":[{\"fieldName\":\"c_class\",\"fieldType\":0},{\"fieldName\":\"c_grade\",\"fieldType\":0},{\"fieldName\":\"c_name\",\"fieldType\":1},{\"fieldName\":\"c_name1\",\"fieldType\":1},{\"fieldName\":\"c_score\",\"fieldType\":1},{\"fieldName\":\"c_score1\",\"fieldType\":1},{\"fieldName\":\"c_score2\",\"fieldType\":1},{\"fieldName\":\"c_score的最小值\",\"fieldType\":1}]}}},{\"type\":\"set_field\",\"customName\":\"字段设置\",\"value\":{\"fieldList\":[{\"fieldId\":\"a_class\",\"transferName\":\"class\",\"fieldType\":0,\"used\":true},{\"fieldId\":\"a_grade\",\"transferName\":\"grade\",\"fieldType\":0,\"used\":true},{\"fieldId\":\"a_name\",\"transferName\":\"a_name 的总数\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"a_name1\",\"transferName\":\"a_name 去重计数\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"a_score\",\"transferName\":\"a_score 总数\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"a_score2\",\"transferName\":\"a_score 最大值\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"a_score的最小值\",\"transferName\":\"a_score的最小值\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"b_class\",\"transferName\":\"b_class\",\"fieldType\":0,\"used\":false},{\"fieldId\":\"b_grade\",\"transferName\":\"b_grade\",\"fieldType\":0,\"used\":false},{\"fieldId\":\"b_name\",\"transferName\":\"b_name 的总数\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"b_name1\",\"transferName\":\"b_name 去重计数\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"b_score\",\"transferName\":\"b_score 总数\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"b_score1\",\"transferName\":\"b_score 平均值\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"b_score2\",\"transferName\":\"b_score 最大值\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"b_score的最小值\",\"transferName\":\"b_score 最小值\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"c_class\",\"transferName\":\"c_class\",\"fieldType\":0,\"used\":false},{\"fieldId\":\"c_grade\",\"transferName\":\"c_grade\",\"fieldType\":0,\"used\":false},{\"fieldId\":\"c_name\",\"transferName\":\"c_name 总数\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"c_name1\",\"transferName\":\"c_name 去重计数\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"c_score\",\"transferName\":\"c_score 总数\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"c_score1\",\"transferName\":\"c_score 平均值\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"c_score2\",\"transferName\":\"c_score 最大值\",\"fieldType\":1,\"used\":true},{\"fieldId\":\"c_score的最小值\",\"transferName\":\"c_score 最小值\",\"fieldType\":1,\"used\":true}]}},{\"type\":\"filter\",\"customName\":\"过滤\",\"value\":{\"filterType\":2,\"children\":[{\"children\":[{\"filterValue\":[{\"value\":\"foo_c\"},{\"value\":\"bar_c\"}],\"fieldId\":\"class\",\"filterType\":13},{\"filterValue\":13,\"fieldId\":\"a_score 总数\",\"filterType\":7},{\"filterValue\":{\"min\":23,\"max\":24},\"fieldId\":\"a_score 最大值\",\"filterType\":3}],\"filterType\":1},{\"filterValue\":\"bar\",\"fieldId\":\"grade\",\"filterType\":15}]}},{\"type\":\"group_by\",\"customName\":\"分组汇总\",\"value\":{\"dimensions\":[{\"fieldId\":\"a_name 的总数\",\"transferName\":\"a_name 的总数-新group\",\"fieldType\":1}],\"norms\":[{\"fieldId\":\"a_score的最小值\",\"transferName\":\"a_score的最小值\",\"fieldType\":1,\"functionType\":4},{\"fieldId\":\"b_score 最小值\",\"transferName\":\"b_score 最小值\",\"fieldType\":1,\"functionType\":4},{\"fieldId\":\"c_score 最小值\",\"transferName\":\"c_score的最小值\",\"fieldType\":1,\"functionType\":4}]}}]");

        getTableOperator.put(2L, "[{\"type\":\"select_field\",\"customName\":\"选字段\",\"value\":{\"tableType\":1,\"tableId\":33,\"tableName\":\"现金表\",\"fields\":[{\"fieldName\":\"部门\",\"columnId\":2,\"fieldType\":0},{\"fieldName\":\"日期\",\"columnId\":3,\"fieldType\":2},{\"fieldName\":\"现金\",\"columnId\":4,\"fieldType\":1},{\"fieldName\":\"收入\",\"columnId\":5,\"fieldType\":1}]}},{\"type\":\"filter\",\"customName\":\"过滤\",\"value\":{\"fieldId\":\"日期\",\"filterType\":31,\"filterValue\":\"date\"}},{\"type\":\"group_by\",\"customName\":\"分组汇总\",\"value\":{\"dimensions\":[{\"fieldId\":\"部门\",\"transferName\":\"depart\",\"fieldType\":0}],\"norms\":[{\"fieldId\":\"现金\",\"transferName\":\"amount\",\"fieldType\":1,\"functionType\":1},{\"fieldId\":\"收入\",\"transferName\":\"income\",\"fieldType\":1,\"functionType\":1}]}}]");
        getTableOperator.put(3L, "[{\"type\":\"select_field\",\"customName\":\"选字段\",\"value\":{\"tableType\":1,\"tableId\":34,\"tableName\":\"用户行为\",\"fields\":[{\"fieldName\":\"部门\",\"columnId\":6,\"fieldType\":0},{\"fieldName\":\"日期\",\"columnId\":7,\"fieldType\":2},{\"fieldName\":\"用户名\",\"columnId\":8,\"fieldType\":0}]}},{\"type\":\"filter\",\"customName\":\"过滤\",\"value\":{\"fieldId\":\"日期\",\"filterType\":31,\"filterValue\":\"date\"}},{\"type\":\"group_by\",\"customName\":\"分组汇总\",\"value\":{\"dimensions\":[{\"fieldId\":\"部门\",\"transferName\":\"depart\",\"fieldType\":0}],\"norms\":[{\"fieldId\":\"用户名\",\"transferName\":\"user_count\",\"fieldType\":1,\"functionType\":6}]}}]");
        getTableOperator.put(4L, "[{\"type\":\"select_field\",\"customName\":\"选字段\",\"value\":{\"tableType\":4,\"tableId\":36,\"tableName\":\"透传过滤器示例-depart_agg\",\"fields\":[{\"fieldName\":\"depart\",\"columnId\":9,\"fieldType\":0},{\"fieldName\":\"现金\",\"columnId\":10,\"fieldType\":1},{\"fieldName\":\"income\",\"columnId\":11,\"fieldType\":1}]}},{\"type\":\"join\",\"customName\":\"左右合并\",\"value\":{\"style\":2,\"basis\":[[\"depart\",\"12\"]],\"table\":{\"tableType\":4,\"tableId\":37,\"tableName\":\"透传过滤器示例-depart_raw\",\"fields\":[{\"fieldName\":\"depart\",\"columnId\":12,\"fieldType\":0},{\"fieldName\":\"user_count\",\"columnId\":13,\"fieldType\":1}]}}}]");

    }

    private List<AnalysisOperator<?>> getTableOperators(Long tableId) {
        try {
            return objectMapper.readValue(getTableOperator.get(tableId), new TypeReference<List<AnalysisOperator<?>>>() {
            });
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw  new RuntimeException(e);
        }
    }

    private String getPhysicsTableName(Long tableId) {
        Map<Long, String> map = new HashMap<>();
        map.put(33L, "depart_agg");
        map.put(34L, "depart_raw");
        map.put(36L, "透传过滤器示例-depart_agg");
        map.put(37L, "透传过滤器示例-depart_raw");
        return map.get(tableId);
    }

    private Map<Long, String> getPhysicsFieldName(Long tableId) {
        Map<Long, Map<Long, String>> map = new HashMap<>();
        map.put(33L, new HashMap<Long, String>() {{
            put(2L, "depart");
            put(3L, "date");
            put(4L, "amount");
            put(5L, "income");
        }});
        map.put(34L, new HashMap<Long, String>() {{
            put(6L, "depart");
            put(7L, "date");
            put(8L, "user");
        }});
        map.put(36L, new HashMap<Long, String>() {{
            put(9L, "depart");
            put(10L, "amount");
            put(11L, "income");
        }});
        map.put(37L, new HashMap<Long, String>() {{
            put(12L, "depart");
            put(13L, "user_count");
        }});
        return map.get(tableId);
    }

    @Test
    public void check() {
        List<AnalysisOperator<?>> operators = getTableOperators(1L);
        OperatorSqlProxy operatorSqlProxy = new OperatorSqlProxy(operators.subList(0, 6), objectMapper);
        operatorSqlProxy.check().forEach(System.out::println);
    }

    @Test
    public void db() {

        List<AnalysisOperator<?>> operators = getTableOperators(11L);
        OperatorSqlProxy operatorSqlProxy = new OperatorSqlProxy(operators.subList(0, 6), objectMapper);
        operatorSqlProxy.getFields().forEach(System.out::println);

        operatorSqlProxy.setBuildSqlWithPlaceHolder(true);
        System.out.println(operatorSqlProxy.getBuildSql());

        operatorSqlProxy.getRelationTables().forEach(System.out::println);
    }

    @Test
    public void tableA() {

        List<AnalysisOperator<?>> operators = getTableOperators(12L);
        OperatorSqlProxy operatorSqlProxy = new OperatorSqlProxy(operators.subList(0, 2), objectMapper, this::getTableOperators);
        operatorSqlProxy.getFields().forEach(System.out::println);

        operatorSqlProxy.setBuildSqlWithPlaceHolder(true);
        System.out.println(operatorSqlProxy.getBuildSql());
    }

    @Test
    public void tableB() {

        List<AnalysisOperator<?>> operators = getTableOperators(13L);
        OperatorSqlProxy operatorSqlProxy = new OperatorSqlProxy(operators.subList(0, 2), objectMapper, this::getTableOperators);
        operatorSqlProxy.getFields().forEach(System.out::println);

        operatorSqlProxy.setBuildSqlWithPlaceHolder(true);
        System.out.println(operatorSqlProxy.getBuildSql());
    }

    @Test
    public void tableC() {

        List<AnalysisOperator<?>> operators = getTableOperators(14L);
        OperatorSqlProxy operatorSqlProxy = new OperatorSqlProxy(operators.subList(0, 2), objectMapper, this::getTableOperators);
        operatorSqlProxy.getFields().forEach(System.out::println);

        operatorSqlProxy.setBuildSqlWithPlaceHolder(true);
        System.out.println(operatorSqlProxy.getBuildSql());
    }

    @Test
    public void joinAbc() {

        List<AnalysisOperator<?>> operators = getTableOperators(15L);
        OperatorSqlProxy operatorSqlProxy = new OperatorSqlProxy(operators.subList(0, 6), objectMapper, this::getTableOperators);
        operatorSqlProxy.getFields().forEach(System.out::println);

        operatorSqlProxy.check().forEach(System.out::println);

        System.out.println(operatorSqlProxy.getBuildSql());

        operatorSqlProxy.getRelationTables().forEach(System.out::println);
    }

    @Test
    public void placeholderAgg() {

        List<AnalysisOperator<?>> operators = getTableOperators(36L);
        OperatorSqlProxy operatorSqlProxy = new OperatorSqlProxy(operators.subList(0, 3), objectMapper, this::getTableOperators);
        operatorSqlProxy.getFields().forEach(System.out::println);

        operatorSqlProxy.setBuildSqlWithPlaceHolder(true);
        System.out.println(operatorSqlProxy.getBuildSql());
    }

    @Test
    public void placeholderRaw() {

        List<AnalysisOperator<?>> operators = getTableOperators(37L);
        OperatorSqlProxy operatorSqlProxy = new OperatorSqlProxy(operators.subList(0, 3), objectMapper, this::getTableOperators);
        operatorSqlProxy.getFields().forEach(System.out::println);

        operatorSqlProxy.setBuildSqlWithPlaceHolder(true);
        System.out.println(operatorSqlProxy.getBuildSql());
    }

    @Test
    public void placeholderJoin() {

        List<AnalysisOperator<?>> operators = getTableOperators(38L);
        OperatorSqlProxy operatorSqlProxy = new OperatorSqlProxy(operators.subList(0, 2), objectMapper, this::getTableOperators);
        operatorSqlProxy.getFields().forEach(System.out::println);

        operatorSqlProxy.setBuildSqlWithPlaceHolder(true);
        System.out.println(operatorSqlProxy.getBuildSql());

        operatorSqlProxy.getPlaceHolders().forEach(System.out::println);
        operatorSqlProxy.getRelationTables().forEach(System.out::println);

        System.out.println(operatorSqlProxy.getBuildSql());
        operatorSqlProxy.getFields().forEach(System.out::println);
        System.out.println(operatorSqlProxy.getBuildSql());

        operatorSqlProxy.reset();
        System.out.println(operatorSqlProxy.getBuildSql());
        operatorSqlProxy.getFields().forEach(System.out::println);
        System.out.println(operatorSqlProxy.getBuildSql());

        operatorSqlProxy.reset();
        operatorSqlProxy.setTableRowCount(2000);
        System.out.println(operatorSqlProxy.getBuildSql());

        operatorSqlProxy.reset();
        operatorSqlProxy.setTableRowCount(2000);
        operatorSqlProxy.setOffset(0, 100);
        System.out.println(operatorSqlProxy.getBuildSql());

        operatorSqlProxy.getPlaceHolders().forEach(System.out::println);
        operatorSqlProxy.getRelationTables().forEach(System.out::println);
    }

    @Test
    public void transferNamesAgg() {
        List<AnalysisOperator<?>> operators = getTableOperators(2L);
        TableTransferOperatorSqlProxy tableTransferOperatorSqlProxy =
                new TableTransferOperatorSqlProxy(operators.subList(0, 3), objectMapper, this::getTableOperators,
                        this::getPhysicsTableName, this::getPhysicsFieldName);

        tableTransferOperatorSqlProxy.getFields().forEach(System.out::println);

        System.out.println(tableTransferOperatorSqlProxy.getBuildSql());

        tableTransferOperatorSqlProxy.reset();
        tableTransferOperatorSqlProxy.setBuildSqlWithPlaceHolder(true);
        System.out.println(tableTransferOperatorSqlProxy.getBuildSql());
    }

    @Test
    public void transferNamesRaw() {
        List<AnalysisOperator<?>> operators = getTableOperators(3L);
        TableTransferOperatorSqlProxy tableTransferOperatorSqlProxy =
                new TableTransferOperatorSqlProxy(operators.subList(0, 3), objectMapper, this::getTableOperators,
                        this::getPhysicsTableName, this::getPhysicsFieldName);

        tableTransferOperatorSqlProxy.getFields().forEach(System.out::println);

        System.out.println(tableTransferOperatorSqlProxy.getBuildSql());

        tableTransferOperatorSqlProxy.reset();
        tableTransferOperatorSqlProxy.setBuildSqlWithPlaceHolder(true);
        System.out.println(tableTransferOperatorSqlProxy.getBuildSql());
    }

    @Test
    public void transferNamesPlaceholderJoin() {
        List<AnalysisOperator<?>> operators = getTableOperators(4L);
        TableTransferOperatorSqlProxy tableTransferOperatorSqlProxy =
                new TableTransferOperatorSqlProxy(operators.subList(0, 2), objectMapper, this::getTableOperators,
                        this::getPhysicsTableName, this::getPhysicsFieldName);

        tableTransferOperatorSqlProxy.getFields().forEach(System.out::println);

        System.out.println(tableTransferOperatorSqlProxy.getBuildSql());

        tableTransferOperatorSqlProxy.reset();
        tableTransferOperatorSqlProxy.setBuildSqlWithPlaceHolder(true);
        System.out.println(tableTransferOperatorSqlProxy.getBuildSql());

        tableTransferOperatorSqlProxy.getPlaceHolders().forEach(System.out::println);
        tableTransferOperatorSqlProxy.getRelationTables().forEach(System.out::println);
    }


}
