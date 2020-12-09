package nx.flink03;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class CountWindowAverageWithValueState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {
    private ValueState<Tuple2<Long, Long>> countAndSum;

    @Override
    public void open(Configuration parameters) throws Exception {
        //注册状态
        ValueStateDescriptor<Tuple2<Long, Long>> descriptor = new ValueStateDescriptor<>(
                "average",
                Types.TUPLE(Types.LONG, Types.LONG)
        );
        countAndSum = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Double>> out) throws Exception {

        // 拿到当前Key 的状态值
        Tuple2<Long, Long> curState = countAndSum.value();
        // 初始化（如果没有该Key）
        if (curState == null) {
            curState = Tuple2.of(0L, 0L);
        }
        //跟新状态值 个数
        curState.f0 += 1;

        // 跟新状态值中的总值
        curState.f1 += element.f1;

        // 跟新状态
        countAndSum.update(curState);

        if (curState.f0 >= 3) {
            double avg = (double) curState.f1 / curState.f0;
            out.collect(Tuple2.of(element.f0, avg));
            countAndSum.clear();
        }
    }
}


