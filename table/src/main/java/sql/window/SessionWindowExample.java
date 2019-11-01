package sql.window;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.sql.Timestamp;
import java.util.Arrays;

public class SessionWindowExample {

    public static void main(String[] args) throws Exception {

        // 获取 environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 指定系统时间概念为 event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        // 初始数据
        DataStream<Tuple3<Long, String,Integer>> log = env.fromCollection(Arrays.asList(
                //时间 14:53:00
                new Tuple3<>(1572591180_000L,"xiao_ming",300),

                /*    Start Session   */
                //时间 14:53:09
                new Tuple3<>(1572591189_000L,"zhang_san",303),
                //时间 14:53:12
                new Tuple3<>(1572591192_000L, "xiao_li",204),

                /*    Start Session   */
                //时间 14:53:21
                new Tuple3<>(1572591201_000L,"li_si", 208)
        ));

        // 指定时间戳
        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> logWithTime = log.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple3<Long, String, Integer>>() {

            @Override
            public long extractAscendingTimestamp(Tuple3<Long, String, Integer> element) {
                return element.f0;
            }
        });

        // 转换为 Table
        Table logT = tEnv.fromDataStream(logWithTime, "t.rowtime, name, v");

        // SESSION(time_attr, interval)
        // interval 表示两条数据触发session的最大间隔
        Table result = tEnv.sqlQuery("SELECT SESSION_START(t, INTERVAL '5' SECOND) AS window_start," +
                "SESSION_END(t, INTERVAL '5' SECOND) AS window_end, SUM(v) FROM "
                + logT + " GROUP BY SESSION(t, INTERVAL '5' SECOND)");

        TypeInformation<Tuple3<Timestamp,Timestamp,Integer>> tpinf = new TypeHint<Tuple3<Timestamp,Timestamp,Integer>>(){}.getTypeInfo();
        tEnv.toAppendStream(result, tpinf).print();

        env.execute();
    }
}
