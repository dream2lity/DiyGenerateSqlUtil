package org.example.sql.base;

import org.example.enums.FieldTypeEnum;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author chijiuwang
 */
@AllArgsConstructor
@NoArgsConstructor
@Data
public class PlaceHolderInfo {

    private String placeholderName;
    private String fieldExpr;
    private FieldTypeEnum fieldType;
    private String fieldId;

}
