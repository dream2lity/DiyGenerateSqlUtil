package org.example.sql.base;

import com.alibaba.druid.sql.ast.SQLExpr;
import org.example.enums.FieldTypeEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 新-解析operators字段模型
 *
 * @author chijiuwang
 */
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Data
public class OperatorField {
    /**
     * 全局唯一fieldId
     */
    private String fieldId;
    private FieldTypeEnum fieldType;
    /**
     * 在当前查询字段列表中的名称
     */
    private String transferName;
    /**
     * 在当前查询字段列表中名称重复的次数
     */
    private int repeatCount;
    /**
     * 所属查询列表层级
     */
    private int level;
    private SQLExpr sqlExpr;
    /**
     * 字段别名
     */
    private String alias;
    /**
     * 来源字段id
     */
    private OperatorField sourceField;

    public OperatorField(FieldTypeEnum fieldType, String transferName, int repeatCount, OperatorField sourceField) {
        this.fieldType = fieldType;
        this.transferName = transferName;
        this.repeatCount = repeatCount;
        this.sourceField = sourceField;
        this.fieldId = generateId();
    }

    public OperatorField(FieldTypeEnum fieldType, String transferName, int repeatCount, SQLExpr sqlExpr, String alias, OperatorField sourceField) {
        this.fieldType = fieldType;
        this.transferName = transferName;
        this.repeatCount = repeatCount;
        this.sqlExpr = sqlExpr;
        this.alias = alias;
        this.sourceField = sourceField;
        this.fieldId = generateId();
    }

    private String generateId() {
        // alias 全局唯一
        // level + transferName + repeatCount 全局唯一
        // level 和 alias 在生成SQL时才能确定
        // transferName + repeatCount 当前查询层唯一
        return getRealTransferName();
    }

    public String getRealTransferName() {
        return repeatCount > 0 ? transferName + repeatCount : transferName;
    }
}
