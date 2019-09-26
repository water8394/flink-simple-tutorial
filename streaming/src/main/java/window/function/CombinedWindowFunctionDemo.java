package window.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class CombinedWindowFunctionDemo {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 构建输入数据
        List<Tuple2<Long, Long>> data = new ArrayList<>();
        Tuple2<Long, Long> a = new Tuple2<>(1L, 1L);
        Tuple2<Long, Long> b = new Tuple2<>(3L, 1L);
        data.add(a);
        data.add(b);
        DataStreamSource<Tuple2<Long, Long>> input = env.fromCollection(data);


        input.keyBy(x -> x.f1)
                .timeWindow(Time.seconds(10), Time.seconds(1))
                // 第一个Function为 ReduceFunction, 取窗口的最小值
                .reduce((r1, r2) -> {
                    return r1.f0 < r2.f0 ? r1 : r2;
                    // 第二个Function为 ProcessWindowFunction, 获取窗口的时间信息
                }, new ProcessWindowFunction<Tuple2<Long, Long>, String, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong, Context context, Iterable<Tuple2<Long, Long>> elements, Collector<String> out) throws Exception {
                        out.collect("window: " + context.window());
                    }
                }).print();

        env.execute();
    }

}
