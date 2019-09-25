package window.common;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.ArrayList;
import java.util.List;

public class SessionWindow {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 构建输入数据, 希望目标是实现 3s 的 Session Gap
        final List<Tuple3<String, Long, Integer>> input = new ArrayList<>();

        input.add(new Tuple3<>("a", 1L, 1));
        input.add(new Tuple3<>("b", 1L, 1));
        input.add(new Tuple3<>("b", 3L, 1));
        input.add(new Tuple3<>("b", 5L, 1));
        input.add(new Tuple3<>("c", 6L, 1));
        // 下面 'a'和 'c' 的本次出现实现与上一次已经超过了 3s, 因此应该是一个新的窗口的起点
        input.add(new Tuple3<>("a", 10L, 1));
        input.add(new Tuple3<>("c", 11L, 1));

        DataStream<Tuple3<String, Long, Integer>> source = env
                .addSource(new SourceFunction<Tuple3<String, Long, Integer>>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public void run(SourceContext<Tuple3<String, Long, Integer>> ctx) throws Exception {
                        for (Tuple3<String, Long, Integer> value : input) {
                            ctx.collectWithTimestamp(value, value.f1);
                            ctx.emitWatermark(new Watermark(value.f1 - 1));
                        }
                        ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
                    }

                    @Override
                    public void cancel() {
                    }
                });

        // 创建 Session Window, 间隔为 3s
        DataStream<Tuple3<String, Long, Integer>> aggregated = source
                .keyBy(0)
                .window(EventTimeSessionWindows.withGap(Time.seconds(3L)))
                .sum(2);

        System.out.println("Printing result to stdout. Use --output to specify output path.");
        aggregated.print();

        env.execute();
    }
}
