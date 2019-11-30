package udf.function;

import org.apache.flink.table.functions.AggregateFunction;

import java.util.Iterator;

public class Mean extends AggregateFunction<Integer, MeanValue> {

    @Override
    public Integer getValue(MeanValue accumulator) {
        return accumulator.sum/accumulator.count;
    }

    @Override
    public MeanValue createAccumulator() {
        return new MeanValue();
    }

    public void accumulate(MeanValue acc, int iValue) {
        acc.sum += iValue;
        acc.count ++;
    }
    public void merge(MeanValue acc, Iterable<MeanValue> it) {
        Iterator<MeanValue> iter = it.iterator();
        while (iter.hasNext()) {
            MeanValue a = iter.next();
            acc.count += a.count;
            acc.sum += a.sum;
        }
    }
    public void resetAccumulator(MeanValue acc) {
        acc.count = 0;
        acc.sum = 0;
    }

}
