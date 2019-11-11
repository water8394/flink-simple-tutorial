package dataSource;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaDataSource {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置 kafka 连接参数
        String topic = "flink";
        String bootStrapServers = "192.168.56.103:9092";
        String zkConnect = "192.168.56.103:2181";
        String groupID = "group_A";
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", bootStrapServers);
//        prop.setProperty("zookeeper.connect", zkConnect);
        prop.setProperty("group.id", groupID);

        // 创建 kafka connector source
//        FlinkKafkaConsumer010<String> consumer010 = new FlinkKafkaConsumer010<>(topic, new SimpleStringSchema(), prop);
        FlinkKafkaConsumer<String> stringFlinkKafkaConsumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), prop);

        // add source
        DataStreamSource<String> dataStream = env.addSource(stringFlinkKafkaConsumer);

        dataStream.print();

        env.execute("Flink kafka test");
    }
}
