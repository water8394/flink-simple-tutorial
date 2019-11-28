package udf.function;


import org.apache.flink.table.functions.ScalarFunction;

public class StringLength extends ScalarFunction {

    public int eval(String s){

        if (s == null) {
            return 0;
        }
        return s.length();
    }
}
