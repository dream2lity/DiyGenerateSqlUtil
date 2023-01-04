package org.example.sql.base;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author chijiuwang
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class RelationTableInfo {
    private Long tableId;
    private String permissionPlaceholderName;
}
