package nx.flink03;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestOperatorStateMain {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enc = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, Integer>> dataStreamSource = enc.fromElements(Tuple2.of("Spark", 3), Tuple2.of("Hadoop", 5), Tuple2.of("Hadoop", 7), Tuple2.of("Spark", 4));


        dataStreamSource.addSink(new CustomSink(2)).setParallelism(1);

        enc.execute("TestOperatorStateMain");
    }
}
