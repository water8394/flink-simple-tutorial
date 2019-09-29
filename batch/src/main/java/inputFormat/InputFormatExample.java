package inputFormat;

import inputFormat.pojo.Info;
import inputFormat.pojo.Item;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.PojoCsvInputFormat;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.core.fs.Path;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;

/**
 * @author XINZE
 * 使用 CsvInputFormat 创建模拟数据源  使用join合并两个dataSet
 *
 */
public class InputFormatExample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // item dataSet
        String itemPath = "item.csv";
        String[] itemField = new String[]{"id", "price"};
        DataSet<Item> items = getSource(env, itemPath, itemField, Item.class);

        // info dataSet
        String infoPath = "info.csv";
        String[] infoField = new String[]{"id", "color", "country"};
        DataSet<Info> infos = getSource(env, infoPath, infoField, Info.class);
        // 关联两个dataset
        JoinOperator.DefaultJoin<Item, Info> dataSet = items.join(infos).where("id").equalTo("id");
        // 使用 joinFunction 处理合并后的两个dataSet
        dataSet.with(new JoinFunction<Item, Info, String>() {
            @Override
            public String join(Item item, Info info) throws Exception {
                return "商品ID:" + item.getId() + " 价格:"+item.getPrice() + " 颜色:"+ info.getColor() + " 国家:" + info.getCountry();
            }
        }).print();

    }

    private static <T> DataSet<T> getSource(ExecutionEnvironment env, String path, String[] fieldOrder, Class<T> type) throws URISyntaxException {

        // 本地文件路径
        URL fileUrl = InputFormatExample.class.getClassLoader().getResource(path);
        Path filePath = Path.fromLocalFile(new File(fileUrl.getPath()));
        // 抽取  TypeInformation，是一个 PojoTypeInfo
        PojoTypeInfo<T> pojoType = (PojoTypeInfo<T>) TypeExtractor.createTypeInfo(type);
        // 由于 Java 反射抽取出的字段顺序是不确定的，需要显式指定下文件中字段的顺序
        // 创建 PojoCsvInputFormat
        PojoCsvInputFormat<T> csvInput = new PojoCsvInputFormat<>(filePath, pojoType, fieldOrder);
        return env.createInput(csvInput, pojoType);
    }


}
