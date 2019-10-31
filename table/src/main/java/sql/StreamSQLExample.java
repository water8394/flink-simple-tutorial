package sql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Arrays;

public class StreamSQLExample {

    public static void main(String[] args) throws Exception {

        // 获取 environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);


        DataStream<Order> orderA = env.fromCollection(Arrays.asList(
                new Order(1L, "beer", 3),
                new Order(1L, "diaper", 4),
                new Order(3L, "rubber", 2)));

        DataStream<Order> orderB = env.fromCollection(Arrays.asList(
                new Order(2L, "pen", 3),
                new Order(2L, "rubber", 3),
                new Order(4L, "beer", 1)));

        // 将 DataStream 转换为 Table
        Table tableA = tEnv.fromDataStream(orderA, "user, product, amount");
        // 将 DataStream 注册成 Table
        tEnv.registerDataStream("OrderB", orderB, "user, product, amount");

        // union 两个 table
        Table result = tEnv.sqlQuery("SELECT * FROM " + tableA + " WHERE amount > 2 UNION ALL " +
                "SELECT * FROM OrderB WHERE amount > 2");

        tEnv.toAppendStream(result, Order.class).print();

        env.execute();
    }

    /**
     * Simple POJO.
     */
    public static class Order {
        public Long user;
        public String product;
        public int amount;

        public Order() {
        }

        public Order(Long user, String product, int amount) {
            this.user = user;
            this.product = product;
            this.amount = amount;
        }

        @Override
        public String toString() {
            return "Order{" +
                    "user=" + user +
                    ", product='" + product + '\'' +
                    ", amount=" + amount +
                    '}';
        }
    }
}