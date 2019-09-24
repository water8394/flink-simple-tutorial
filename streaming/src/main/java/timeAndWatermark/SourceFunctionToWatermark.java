package timeAndWatermark;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * @author XINZE
 *
 * 在 Source Function 中 直接指定 Timestamps 和 Watermark
 */
public class SourceFunctionToWatermark {
    public static void main(String[] args) throws Exception {


        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        // 添加数组作为数据输入源
        String[] elementInput = new String[]{"hello Flink, 17788900", "Second Line, 17788923"};
        DataStream<String> text = env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                for (String s : elementInput) {
                    // 切割每一条数据
                    String[] inp = s.split(",");
                    Long timestamp = new Long(inp[1]);
                    // 生成 event time 时间戳
                    ctx.collectWithTimestamp(s, timestamp);
                    // 调用 emitWatermark() 方法生成 watermark, 最大延迟设定为 2
                    ctx.emitWatermark(new Watermark(timestamp - 2));
                }
                // 设定默认 watermark
                ctx.emitWatermark(new Watermark(Long.MAX_VALUE));
            }

            @Override
            public void cancel() {

            }
        });


        text.print();

        env.execute("Inside DataSource Demo");


    }

}
