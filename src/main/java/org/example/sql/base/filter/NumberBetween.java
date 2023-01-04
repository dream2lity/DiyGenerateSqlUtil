package org.example.sql.base.filter;

import lombok.Data;

/**
 * 数值型字段在过滤操作中的区间参数模型
 * @author chijiuwang
 */
@Data
public class NumberBetween {
    /**
     * 最小值
     */
    private Double min;
    /**
     * 最大值
     */
    private Double max;
}
