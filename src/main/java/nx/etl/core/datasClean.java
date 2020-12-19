package nx.etl.core;

import com.sun.xml.internal.bind.v2.TODO;
import nx.etl.source.custRedisSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Properties;

public class datasClean {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092");
        prop.put("group.id", "nxflink_consumer");
        prop.put("enable.auto.commit", "false");
        prop.put("auto.offset.reset", "earliest");
        String topic = "nxflink";


        //TODO kafkaSource
        FlinkKafkaConsumer011<String> cons011 = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), prop);

        //TODO RedisSource


        DataStreamSource<String> kafkaConsumer011 = env.addSource(cons011);


        DataStream<HashMap<String, String>> redisSource = env.addSource(new custRedisSource()).broadcast();


        SingleOutputStreamOperator<String> streamOperator = kafkaConsumer011.connect(redisSource).flatMap(new ETLProcessFunction());


        //TODO write to Kafka Sink

        String outputTopic = "adc";
        Properties producerProperties = new Properties();
        producerProperties.put("bootstrap.servers", "localhost:9092");
        FlinkKafkaProducer011<String> producer = new FlinkKafkaProducer011<>(outputTopic,
                new KeyedSerializationSchemaWrapper<String>(new SimpleStringSchema()),
                producerProperties);

//        streamOperator.print();

        streamOperator.addSink(producer);


        env.execute("datasClean");
    }

}
