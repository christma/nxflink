package nx.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        String topic = "nxflink";
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers", "192.168.208.51:9092");
        consumerProperties.setProperty("group.id", "nxflink_consumer");
        FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), consumerProperties);
        DataStreamSource<String> data = env.addSource(myConsumer).setParallelism(3);
        SingleOutputStreamOperator<Tuple2<String, Integer>> wc = data.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] feilds = line.split(",");
                for (String word : feilds) {
                    collector.collect(new Tuple2<>(word, 1));
                }
            }
        }).setParallelism(2);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = wc.keyBy(0).sum(1).setParallelism(2);
        result.map(tuple -> tuple.toString()).setParallelism(2).print().setParallelism(2);


        env.execute("kafka wc");
    }
}
