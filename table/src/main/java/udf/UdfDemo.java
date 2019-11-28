package udf;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import udf.function.StringLength;
import udf.function.StringSplit;

public class UdfDemo {

    public static void main(String[] args) throws Exception {


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSource<String> ds = env.fromElements("Hello#aaaaa","bbbb#ccc");
        Table table = tEnv.fromDataSet(ds,"a");
        tEnv.registerTable("t",table);


        tEnv.registerFunction("stringLength",new StringLength());
        tEnv.registerFunction("split", new StringSplit());


        //Table result = tEnv.sqlQuery("select word,stringLength(word) from " + table);

        Table result = tEnv.sqlQuery("SELECT a,word, length FROM t, LATERAL TABLE(split(a)) as T(word, length)");

        TypeInformation<Tuple3<String,String,Integer>> tpinf = new TypeHint<Tuple3<String,String,Integer>>(){}.getTypeInfo();
        //TypeInformation<String> tpinf = new TypeHint<String>(){}.getTypeInfo();
        tEnv.toDataSet(result,tpinf).print();



    }

}
