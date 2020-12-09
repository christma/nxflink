package nx.flink03;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class SumFunction extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {
    private ReducingState<Long> sumState;


    @Override
    public void open(Configuration parameters) throws Exception {

        ReducingStateDescriptor<Long> descriptor = new ReducingStateDescriptor<>(
                "sum",
                new ReduceFunction<Long>() {
                    @Override
                    public Long reduce(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                },
                Long.class
        );

        sumState = getRuntimeContext().getReducingState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Long>> out) throws Exception {

        sumState.add(element.f1);
        out.collect(Tuple2.of(element.f0, sumState.get()));
    }
}
