package org.example.sql.function.impl.functions;

import org.antlr.v4.runtime.tree.ParseTree;
import org.example.sql.function.ColumnELParser;
import org.example.sql.function.impl.ColumnELConvertListener;
import org.example.sql.function.impl.functions.impl.*;
import org.reflections.Reflections;

import java.util.*;
import java.util.function.Function;

/**
 * @author chijiuwang
 */
public class FunctionFactory {

    private final Map<String, Class<? extends AbstractFunctionHandler>> FUNCTION_MAP = new HashMap<>();

    public FunctionFactory() {
        Reflections reflections = new Reflections(AbstractFunctionHandler.class.getPackage().getName());
        Set<Class<? extends AbstractFunctionHandler>> types = reflections.getSubTypesOf(AbstractFunctionHandler.class);
        int length = AbstractFunctionHandler.class.getSimpleName().length();
        types.forEach(t -> {
            String s = t.getSimpleName().substring(0, t.getSimpleName().length() - length).toUpperCase();
            FUNCTION_MAP.put(s, t);
        });
    }

    public static AbstractFunctionHandler newInstance(ColumnELParser.FunctionExpressionContext ctx,
                                                      Function<ParseTree, ColumnELConvertListener.TreeNodeInfo> treeTreeNodeInfoFunction)
            throws InstantiationException, IllegalAccessException {
        String functionName = ctx.FunctionName().getText();
        FunctionEnum functionEnum = FunctionEnum.valueOf(functionName.toUpperCase());
        AbstractFunctionHandler functionHandler = functionEnum.getClassType().newInstance();
        functionHandler.setCtx(ctx);
        functionHandler.setTreeTreeNodeInfoFunction(treeTreeNodeInfoFunction);
        return functionHandler;
    }

    public enum FunctionEnum {
        /**
         * 逻辑函数
         */
        IF("IF", IfFunctionHandler.class),
        SWITCH("SWITCH", SwitchFunctionHandler.class),
        AND("AND", AndFunctionHandler .class),
        OR("OR", OrFunctionHandler.class),

        /**
         * 数字函数
         */
        MAX("MAX", MaxFunctionHandler.class),
        MIN("MIN", MinFunctionHandler.class),
        MOD("MOD", ModFunctionHandler.class),
        POWER("POWER", PowerFunctionHandler.class),
        ROUND("ROUND", RoundFunctionHandler.class),
        RANDBETWEEN("RANDBETWEEN", RandBetweenFunctionHandler.class),

        /**
         * 文本函数
         */
        UPPER("UPPER", UpperFunctionHandler.class),
        LOWER("LOWER", LowerFunctionHandler.class),
        TRIM("TRIM", TrimFunctionHandler.class),
        CONCAT("CONCAT", ConcatFunctionHandler.class),
        FORMAT("FORMAT", FormatFunctionHandler.class),
        MID("MID", MidFunctionHandler.class),
        REPLACE("REPLACE", ReplaceFunctionHandler.class),

        /**
         * 时间函数
         */
        NOW("NOW", NowFunctionHandler.class),
        DATEDIF("DATEDIF", DateDifFunctionHandler.class),
        DATEDELAY("DATEDELAY", DateDelayFunctionHandler.class),
        DATETOLONG("DATETOLONG", DateToLongFunctionHandler.class),
        TIME("TIME", TimeFunctionHandler.class),

        /**
         * 其他函数
         */
        ISNULL("ISNULL", IsNullFunctionHandler.class),
        NVL("NVL", NvlFunctionHandler.class),
        ;

        String functionName;
        Class<? extends AbstractFunctionHandler> classType;

        FunctionEnum(String functionName, Class<? extends AbstractFunctionHandler> classType) {
            this.functionName = functionName;
            this.classType = classType;
        }

        public String getFunctionName() {
            return functionName;
        }

        public Class<? extends AbstractFunctionHandler> getClassType() {
            return classType;
        }

        public static List<FunctionEnum> getLogicFunctionEnums() {
            return Arrays.asList(IF, SWITCH, AND, OR);
        }

        public static List<FunctionEnum> getNumberFunctionEnums() {
            return Arrays.asList(MAX, MIN, MOD, POWER, ROUND, RANDBETWEEN);
        }

        public static List<FunctionEnum> getTextFunctionEnums() {
            return Arrays.asList(UPPER, LOWER, TRIM, CONCAT, FORMAT, MID, REPLACE);
        }

        public static List<FunctionEnum> getDateFunctionEnums() {
            return Arrays.asList(NOW, DATEDIF, DATEDELAY, DATETOLONG, TIME);
        }

        public static List<FunctionEnum> getOtherFunctionEnums() {
            return Arrays.asList(ISNULL, NVL);
        }
    }

}
