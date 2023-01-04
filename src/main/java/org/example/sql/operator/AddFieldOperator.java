package org.example.sql.operator;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.example.enums.AnalysisOperatorEnum;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * @author chijiuwang
 */
@EqualsAndHashCode(callSuper = true)
@Data
public class AddFieldOperator extends AnalysisOperator<List<AddFieldOperator.AddFieldInfo<?>>> {

    public AddFieldOperator() {
        this.type = AnalysisOperatorEnum.add_field;
        this.customName = AnalysisOperatorEnum.add_field.getDesc();
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.EXISTING_PROPERTY, property = "type", visible = true)
    @JsonSubTypes({
            @JsonSubTypes.Type(value = AddFunctionFieldInfo.class, name = "function"),
            @JsonSubTypes.Type(value = AddGroupByFieldInfo.class, name = "group_by"),
            @JsonSubTypes.Type(value = AddTimeDiffFieldInfo.class, name = "time_diff"),
            @JsonSubTypes.Type(value = AddFetchTimeFieldInfo.class, name = "fetch_time"),
            @JsonSubTypes.Type(value = AddDivideNumberDiyFieldInfo.class, name = "divide_number_diy"),
            @JsonSubTypes.Type(value = AddDivideNumberAutoFieldInfo.class, name = "divide_number_auto"),
            @JsonSubTypes.Type(value = AddDivideTextFieldInfo.class, name = "divide_text"),
    })
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @Data
    public static class AddFieldInfo<T> {
        protected String name;
        protected Integer fieldType;
        protected String text;
        protected String type;
        protected T value;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class AddFunctionFieldInfo extends AddFieldInfo<String> {
        public AddFunctionFieldInfo() {
            this.type = "function";
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class AddGroupByFieldInfo extends AddFieldInfo<AddGroupByFieldInfo.GroupByFieldInfo> {
        public AddGroupByFieldInfo() {
            this.type = "group_by";
        }

        @JsonInclude(JsonInclude.Include.NON_NULL)
        @Data
        public static class GroupByFieldInfo {
            private List<String> dimensionFieldIds;
            private String normFieldId;
            private Integer functionType;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class AddTimeDiffFieldInfo extends AddFieldInfo<AddTimeDiffFieldInfo.TimeDiffInfo> {
        public AddTimeDiffFieldInfo() {
            this.type = "time_diff";
        }

        @JsonInclude(JsonInclude.Include.NON_NULL)
        @Data
        public static class TimeDiffInfo {
            private TimeInfo minuend;
            private TimeInfo minus;
            private Integer unit;

            @JsonInclude(JsonInclude.Include.NON_NULL)
            @Data
            public static class TimeInfo {
                private TimeTypeEnum type;
                private String value;

                public enum TimeTypeEnum {
                    system, fieldId
                }
            }
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class AddFetchTimeFieldInfo extends AddFieldInfo<AddFetchTimeFieldInfo.FetchTimeFieldInfo> {
        public AddFetchTimeFieldInfo() {
            this.type = "fetch_time";
        }

        @JsonInclude(JsonInclude.Include.NON_NULL)
        @Data
        public static class FetchTimeFieldInfo {
            private String fieldId;
            private Integer unit;
        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class AddDivideNumberDiyFieldInfo extends AddFieldInfo<AddDivideNumberDiyFieldInfo.DivideNumberDiyFieldInfo> {
        public AddDivideNumberDiyFieldInfo() {
            this.type = "divide_number_diy";
        }

        @JsonInclude(JsonInclude.Include.NON_NULL)
        @Data
        public static class DivideNumberDiyFieldInfo {
            private String fieldId;
            private String useOther;
            private String max;
            private String min;
            private List<NodeDetail> nodes;

            @JsonInclude(JsonInclude.Include.NON_NULL)
            @Data
            public static class NodeDetail {
                private String groupName;
                private String max;
                private String min;
                private Boolean closeMax;
                private Boolean closeMin;
            }

        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class AddDivideNumberAutoFieldInfo extends AddFieldInfo<AddDivideNumberAutoFieldInfo.DivideNumberAutoFieldInfo> {
        public AddDivideNumberAutoFieldInfo() {
            this.type = "divide_number_auto";
        }

        @JsonInclude(JsonInclude.Include.NON_NULL)
        @Data
        public static class DivideNumberAutoFieldInfo {
            private String fieldId;
            private String groupInterval;
            private String max;
            private String min;

        }
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @EqualsAndHashCode(callSuper = true)
    @Data
    public static class AddDivideTextFieldInfo extends AddFieldInfo<AddDivideTextFieldInfo.DivideTextFieldInfo> {
        public AddDivideTextFieldInfo() {
            this.type = "divide_text";
        }

        @JsonInclude(JsonInclude.Include.NON_NULL)
        @Data
        public static class DivideTextFieldInfo {
            private String fieldId;
            private String useOther;
            private List<Detail> details;

            @JsonInclude(JsonInclude.Include.NON_NULL)
            @Data
            public static class Detail {
                private String groupName;
                private List<String> content;
            }
        }
    }

}
