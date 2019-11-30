package udf;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import udf.function.Mean;
import udf.function.StringLength;
import udf.function.StringSplit;

public class UdfDemo {

    public static void main(String[] args) throws Exception {


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tEnv = BatchTableEnvironment.create(env);

        DataSource<String> ds = env.fromElements("Hello#aaaaa","bbbb#ccc");
        DataSource<Tuple2<String,Integer>> meanDataset
                = env.fromElements(new Tuple2<>("a",2),new Tuple2<>("b",3),new Tuple2<>("a",8));

        Table table = tEnv.fromDataSet(ds,"a");
        tEnv.registerTable("t",table);

        Table t3 = tEnv.fromDataSet(meanDataset,"name,v");
        tEnv.registerTable("t3",t3);

        tEnv.registerFunction("stringLength",new StringLength());
        tEnv.registerFunction("split", new StringSplit());
        tEnv.registerFunction("get_mean", new Mean());

        //tEnv.registerFunction("top2", new Top2());


        //Table result = tEnv.sqlQuery("select word,stringLength(word) from " + table);
        //Table result = tEnv.sqlQuery("SELECT a,word, length FROM t, LATERAL TABLE(split(a)) as T(word, length)");
        Table result = tEnv.sqlQuery("SELECT name,get_mean(v) as mean_value FROM t3 GROUP BY name");


        //TypeInformation<Tuple3<String,String,Integer>> tpinf = new TypeHint<Tuple3<String,String,Integer>>(){}.getTypeInfo();
        //TypeInformation<String> tpinf = new TypeHint<String>(){}.getTypeInfo();
        TypeInformation<Tuple2<String,Integer>> tpinf = new TypeHint<Tuple2<String,Integer>>(){}.getTypeInfo();
        tEnv.toDataSet(result,tpinf).print();



    }

}
