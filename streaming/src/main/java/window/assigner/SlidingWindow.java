package window.assigner;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class SlidingWindow {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 构建输入数据
        List<Tuple2<String, Long>> data = new ArrayList<>();
        Tuple2<String, Long> a = new Tuple2<>("first event", 1L);
        Tuple2<String, Long> b = new Tuple2<>("second event", 2L);
        data.add(a);
        data.add(b);
        DataStreamSource<Tuple2<String, Long>> input = env.fromCollection(data);

        // 使用 ProcessTime 滑动窗口, 10s 为一个窗口长度, 每 1s 滑动一次
        input.keyBy(x -> x.f1)
                .timeWindow(Time.seconds(10), Time.seconds(1))
                .reduce(new MyWindowFunction());

        env.execute();
    }

    public static class MyWindowFunction implements ReduceFunction<Tuple2<String, Long>> {
        @Override
        public Tuple2<String, Long> reduce(Tuple2<String, Long> t1, Tuple2<String, Long> t2) throws Exception {
            return new Tuple2<>(t1.f0 + t2.f0, t1.f1);
        }
    }
}
