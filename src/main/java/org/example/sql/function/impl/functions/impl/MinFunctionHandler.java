package org.example.sql.function.impl.functions.impl;

/**
 * @author chijiuwang
 */
public class MinFunctionHandler extends MaxFunctionHandler {
    public MinFunctionHandler() {
        this.functionName = "MIN";
        this.functionRealName = "LEAST";
    }
}
