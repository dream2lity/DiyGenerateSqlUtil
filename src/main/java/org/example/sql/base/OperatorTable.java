package org.example.sql.base;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @author chijiuwang
 */
@AllArgsConstructor
@Data
public class OperatorTable {
    private String tableName;
    private int repeatCount;
    public String getInnerTableName() {
        return repeatCount > 0 ? tableName + repeatCount : tableName;
    }
}
