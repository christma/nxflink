package nx.flink03;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class CountWindowAverageWithListState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {
    private ListState<Tuple2<Long, Long>> elementsByKey;

    @Override
    public void open(Configuration parameters) throws Exception {
        ListStateDescriptor<Tuple2<Long, Long>> de = new ListStateDescriptor<>(
                "avg",
                Types.TUPLE(Types.LONG, Types.LONG)
        );
        elementsByKey = getRuntimeContext().getListState(de);

    }

    @Override
    public void flatMap(Tuple2<Long, Long> element, Collector<Tuple2<Long, Double>> out) throws Exception {
        Iterable<Tuple2<Long, Long>> curState = elementsByKey.get();
        if (curState == null) {
            elementsByKey.addAll(Collections.emptyList());
        }
        elementsByKey.add(element);


        List<Tuple2<Long, Long>> allElements = Lists.newArrayList(elementsByKey.get());

        if (allElements.size() >= 3) {
            long count = 0;
            long sum = 0;
            for (Tuple2<Long, Long> ele : allElements) {
                count++;
                sum += ele.f1;
            }
            double avg = (double) sum / count;
            out.collect(Tuple2.of(element.f0, avg));
            elementsByKey.clear();
        }
    }
}
