package wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;

public class WordCount {

    public static void main(String[] args) throws Exception {

        // 获取 StreamEnv
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 获取 输入流
        DataStream<String> text = env.fromElements(WordCountData.WORDS);

        // 执行计算Operator
        DataStream<Tuple2<String, Integer>> counts
                = text.flatMap(new SplitFunction())
                .keyBy(0).sum(1);

        // 输出结果
        counts.print();

        // 启动flink程序
        env.execute("WordCount Demo");
    }

    // *************************************************************************
    // 自定义切割Function切分一行输入
    // *************************************************************************
    public static final class SplitFunction implements FlatMapFunction<String, Tuple2<String, Integer>>{

        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] words = s.toLowerCase().split(" ");
            for (String word : words) {
                if (word.length() > 0){
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }
}
