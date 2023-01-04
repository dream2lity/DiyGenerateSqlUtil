package org.example.sql.base;

import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;
import org.example.enums.FieldTypeEnum;

/**
 * @author chijiuwang
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
public class Field {
    /**
     * 字段名称
     */
    private String fieldName;
    /**
     * 字段显示名称
     */
    private String aliasFieldName;
    /**
     * @see FieldTypeEnum
     */
    private int fieldType;
    /**
     * {@link com.sunlands.bi.dataset.dmo.DatasetColumn} 中的column_id
     */
    private Long columnId;
}
