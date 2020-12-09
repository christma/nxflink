package nx.flink03;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestKeyedStateMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Long, Long>> dataStreamSource = env.fromElements(Tuple2.of(1L, 3L), Tuple2.of(1L, 5L),
                Tuple2.of(1L, 7L), Tuple2.of(2L, 4L), Tuple2.of(3L, 2L), Tuple2.of(2L, 5L), Tuple2.of(3L, 5L), Tuple2.of(3L, 5L), Tuple2.of(3L, 5L));

//        dataStreamSource
//                .keyBy("0")
//                .flatMap(new CountWindowAverageWithValueState())
//                .print();
//
//        System.out.println("----------------------------------------------------");
//        dataStreamSource
//                .keyBy("0")
//                .flatMap(new CountWindowAverageWithListState())
//                .print();
//
//        dataStreamSource
//                .keyBy("0")
//                .flatMap(new CountWindowAverageWithMapState())
//                .print();


//        dataStreamSource.keyBy("0").flatMap(new SumFunction()).print();

        dataStreamSource.keyBy("0").flatMap(new ContainsValueFunction()).print();

        env.execute("TestKeyedStateMain");
    }
}
