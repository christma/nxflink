package nx.flink03;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.UUID;

public class CountWindowAverageWithMapState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {
    private MapState<String, Long> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {

        MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>(
                "avg",
                String.class,
                Long.class
        );
        mapState = getRuntimeContext().getMapState(descriptor);
    }


    @Override
    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Double>> out) throws Exception {
        mapState.put(UUID.randomUUID().toString(), element.f1);
        List<Long> allElements = Lists.newArrayList(mapState.values());
        if (allElements.size() >= 3) {
            long count = 0;
            long sum = 0;
            for (Long ele : allElements) {
                count++;
                sum += ele;
            }
            double avg = (double) sum / count;
            out.collect(Tuple2.of(element.f0, avg));// 清除状态

            mapState.clear();
        }
    }
}