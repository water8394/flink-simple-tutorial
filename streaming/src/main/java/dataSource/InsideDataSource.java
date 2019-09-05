package dataSource;

import org.apache.flink.api.java.io.CsvInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

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
        DataStream<String> text3 = env.socketTextStream("localhost", 9999, "\n", 4);


        // 添加文件源
        // 直接读取文本文件
        DataStream<String> text4 = env.readTextFile("/opt/history.log");
        // 指定 CsvInputFormat, 监控csv文件(两种模式), 时间间隔是10ms
        DataStream<String> text5 = env.readFile(new CsvInputFormat<String>(new Path("/opt/history.csv")) {
            @Override
            protected String fillRecord(String s, Object[] objects) {
                return null;
            }
        },"/opt/history.csv", FileProcessingMode.PROCESS_CONTINUOUSLY,10);

        text.print();

        env.execute("Inside DataSource Demo");
    }
}
