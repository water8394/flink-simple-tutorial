package timeAndWatermark;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

import java.util.ArrayList;
import java.util.List;

public class AscendingAssigner {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 指定系统时间概念为 event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        List<Tuple2<String, Long>> collectionInput = new ArrayList<>();
        Tuple2<String, Long> a = new Tuple2<>("first event", 1L);
        Tuple2<String, Long> b = new Tuple2<>("second event", 2L);
        collectionInput.add(a);
        collectionInput.add(b);

        // 使用 Ascending 分配 时间信息和 watermark
        DataStream<Tuple2<String, Long>> text = env.fromCollection(collectionInput);
        text.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<String, Long>>() {
            @Override
            public long extractAscendingTimestamp(Tuple2<String, Long> element) {
                return element.f1;
            }
        });

        env.execute();
    }

}
