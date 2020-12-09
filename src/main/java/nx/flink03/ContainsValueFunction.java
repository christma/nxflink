package nx.flink03;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class ContainsValueFunction extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, String>> {
    private AggregatingState<Long, String> totalStr;

    @Override
    public void open(Configuration parameters) throws Exception {

        AggregatingStateDescriptor<Long, String, String> descriptor = new AggregatingStateDescriptor<>(
                "totalStr",
                new AggregateFunction<Long, String, String>() {
                    @Override
                    public String createAccumulator() {
                        return "Contains_";

                    }

                    @Override
                    public String add(Long value, String acc) {
                        if ("Contains_".equals(acc)) {
                            return acc + value;
                        }
                        return acc + " _ " + value;
                    }

                    @Override
                    public String getResult(String s) {
                        return s;
                    }

                    @Override
                    public String merge(String s, String acc1) {
                        return s + " _ " + acc1;
                    }
                },
                String.class
        );
        totalStr = getRuntimeContext().getAggregatingState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, String>> collector) throws Exception {
        totalStr.add(element.f1);
        collector.collect(Tuple2.of(element.f0, totalStr.get()));
    }
}
