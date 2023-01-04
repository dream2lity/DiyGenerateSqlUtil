package org.example.sql.base;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import lombok.Data;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * @author chijiuwang
 */
@Data
public class WithAnyProperties {
    /**
     * <h2>其他属性，目前用于存储前端的属性，后端不用处理这些属性</h2>
     */
    protected Map<String, Object> anyOtherProperties;

    @JsonAnyGetter
    public Map<String, Object> getAnyOtherProperties() {
        return anyOtherProperties;
    }

    @JsonAnySetter
    public void setAnyOtherProperties(String key, Object value) {
        if (Objects.isNull(this.anyOtherProperties)) {
            this.anyOtherProperties = new LinkedHashMap<>();
        }
        this.anyOtherProperties.put(key, value);
    }
}
