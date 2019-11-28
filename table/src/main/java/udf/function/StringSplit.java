package udf.function;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class StringSplit extends TableFunction<Row> {

    public void eval(String str) {
        for (String s : str.split("#")) {
            Row row = new Row(2);
            row.setField(0, s);
            row.setField(1, s.length());
            collect(row);
        }
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(Types.STRING,Types.INT);
    }

}
