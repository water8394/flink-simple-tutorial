package broadcast;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import java.util.HashMap;
import java.util.List;

/**
 * @author XINZE
 *
 * 使用 Broadcast
 *
 */
public class BroadcastExample {

    public static void main(String[] args) throws Exception {


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        // 创建需要广播的 数据集 (name, age)
        Tuple2<String, Integer> john = new Tuple2<>("john", 23);
        Tuple2<String, Integer> tom = new Tuple2<>("tom", 24);
        Tuple2<String, Integer> shiny = new Tuple2<>("shiny", 22);
        DataSource<Tuple2<String, Integer>> broadcastData = env.fromElements(john, tom, shiny);

        // 新建一个dataset -> d1, 设置并行度为4
        // 此时 d1 是无法访问 broadcastData 的数据的, 因为两个dataset可能不在一个节点或者slot中, 所以 flink 是不允许去访问的
        DataSet<String> d1 = env.fromElements("john", "tom", "shiny").setParallelism(4);

        // 使用 RichMapFunction, 在open() 方法中拿到广播变量
        d1.map(new RichMapFunction<String, String>() {
            List<Tuple2<String, Integer>> bc;
            HashMap<String, Integer> map = new HashMap<>();
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                this.bc = getRuntimeContext().getBroadcastVariable("broadcastData");
                for (Tuple2<String, Integer> tp : bc) {
                    this.map.put(tp.f0, tp.f1);
                }
            }
            @Override
            public String map(String s) throws Exception {
                Integer age = this.map.get(s);
                return s + "->" + age;
            }
        }).withBroadcastSet(broadcastData, "broadcastData").print();

//        env.execute("Broadcast Example");
    }

}
