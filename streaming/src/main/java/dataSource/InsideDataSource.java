package dataSource;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class InsideDataSource {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 添加数组作为数据输入源
        String[] elementInput = new String[]{"hello Flink", "Second Line"};
        DataStream<String> text = env.fromElements(elementInput);

        // 添加List集合作为数据输入源
        List<String> collectionInput = new ArrayList<>();
        collectionInput.add("hello Flink");
        DataStream<String> text2 = env.fromCollection(collectionInput);

        // 添加Socket作为数据输入源
        // 4个参数 -> (hostname:Ip地址, port:端口, delimiter:分隔符, maxRetry:最大重试次数)
        env.socketTextStream("localhost", 9999, "\n", 4);


        text.print();

        env.execute("Inside DataSource Demo");
    }
}
