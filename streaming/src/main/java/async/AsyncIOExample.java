package async;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class AsyncIOExample {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> inp = env.fromElements(AsyncIOData.WORDS);

        // 接收数据
        SingleOutputStreamOperator<String> out = inp.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                System.out.println("读取数据:" + s + "  当前时间:" + System.currentTimeMillis());
                return s;
            }
        });

        // 使用 AsyncFunction 对函数做一个简单的处理, 中间随机睡眠 1-10s
        DataStream<String> asyncStream = AsyncDataStream.unorderedWait(out, new SimpleAsyncFunction(), 20_000L, TimeUnit.MILLISECONDS);

        // 对已经被 AsyncFunction 处理过的数据再输出一次
        asyncStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                System.out.println("数据处理完毕:" + s + "  当前时间:" + System.currentTimeMillis());
                return s;
            }
        });


        env.execute("AsyncFunction Demo");
    }

    public static class SimpleAsyncFunction extends RichAsyncFunction<String, String>{

        private long waitTime;
        private final Random rnd = new Random(hashCode());

        @Override
        public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {
            // 随机睡眠 1 - 10s
            System.out.println("开始 AsyncFunction  target -> " + input);
            waitTime = rnd.nextInt(10);
            Thread.sleep(waitTime * 1000);
            String out = input + input;
            resultFuture.complete(Collections.singletonList(out));
            System.out.println("结束 AsyncFunction  target -> " + input + "  Sleep time = " + waitTime + "s");
        }
    }
}
