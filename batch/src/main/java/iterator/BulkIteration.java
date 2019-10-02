package iterator;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.MapOperator;

import java.util.List;

/**
 * @author XINZE
 *
 * 使用 Bulk iterator API 实现圆周率的计算
 *
 */
public class BulkIteration {

    public static void main(String[] args) throws Exception {


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataSource<Integer> data = env.fromElements(0);

        // 循环数据
        IterativeDataSet<Integer> loop = data.iterate(1000);

        // 计算过程
        MapOperator<Integer, Integer> process = loop.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer i) throws Exception {
                double x = Math.random();
                double y = Math.random();
                int result = (x * x + y * y) < 1 ? 1 : 0;
                return i + result;
            }
        });

        // 使用 closeWith 调用计算过程
        List<Integer> collect = loop.closeWith(process).collect();

        // 输出最终结果
        for (Integer i : collect) {
            System.out.println( i / 1000.0 * 4);
        }

    }

}
