package org.example.sql.base;

import lombok.Data;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chijiuwang
 */
@Data
public class OperatorCheckResult {
    private boolean isValid = true;
    private List<MissFieldInfo> missFieldInfos = new ArrayList<>();
    private List<FieldTypeNotMatchInfo> fieldTypeNotMatchInfos = new ArrayList<>();

    public boolean getIsValid() {
        return isValid;
    }

    public void setIsValid(boolean valid) {
        isValid = valid;
    }

    public void addMissFieldInfo(MissFieldInfo missFieldInfo) {
        this.isValid = false;
        this.missFieldInfos.add(missFieldInfo);
    }

    public void addFieldTypeNotMatchInfo(FieldTypeNotMatchInfo fieldTypeNotMatchInfo) {
        this.isValid = false;
        this.fieldTypeNotMatchInfos.add(fieldTypeNotMatchInfo);
    }

    @Data
    public static class MissFieldInfo {
        private String fieldId;
    }

    @Data
    public static class FieldTypeNotMatchInfo {
        private String fieldId;
    }
}
