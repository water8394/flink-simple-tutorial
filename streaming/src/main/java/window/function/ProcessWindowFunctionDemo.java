package window.function;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class ProcessWindowFunctionDemo {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 构建输入数据
        List<Tuple3<String, Long, Long>> data = new ArrayList<>();
        Tuple3<String, Long, Long> a1 = new Tuple3<>("first event", 1L, 1111L);
        Tuple3<String, Long, Long> a2 = new Tuple3<>("second event", 1L, 1112L);
        Tuple3<String, Long, Long> a3 = new Tuple3<>("third event", 1L, 20121L);
        Tuple3<String, Long, Long> b1 = new Tuple3<>("first event", 2L, 1111L);
        Tuple3<String, Long, Long> b2 = new Tuple3<>("second event", 2L, 1112L);
        Tuple3<String, Long, Long> b3 = new Tuple3<>("third event", 2L, 30111L);
        data.add(a1);
        data.add(a2);
        data.add(a3);
        data.add(b1);
        data.add(b2);
        data.add(b3);
        DataStreamSource<Tuple3<String, Long, Long>> input = env.fromCollection(data);


        input.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<String, Long, Long>>() {
            @Override
            public long extractAscendingTimestamp(Tuple3<String, Long, Long> element) {
                return element.f2;
            }
        }).keyBy(x -> x.f1).timeWindow(Time.seconds(1), Time.seconds(1)).process(new MyProcessWindowFunction()).print();

        // 输出结果:
        // 3> window: TimeWindow{start=1000, end=2000}word count: 4
        // 4> window: TimeWindow{start=1000, end=2000}word count: 4
        // 3> window: TimeWindow{start=20000, end=21000}word count: 2
        // 4> window: TimeWindow{start=30000, end=31000}word count: 2


        env.execute();
    }

    public static class MyProcessWindowFunction extends ProcessWindowFunction<Tuple3<String, Long, Long>, String, Long, TimeWindow> {
        @Override
        public void process(Long s, Context context, Iterable<Tuple3<String, Long, Long>> elements, Collector<String> out) throws Exception {
            // 统计每个窗口内的所有数据的 f0字段加起来共有多少个单词
            Long count = 0L;
            for (Tuple3<String, Long, Long> element : elements) {
                count += element.f0.split(" ").length;
            }
            out.collect("window: " + context.window() + " word count: " + count);
        }
    }
}
