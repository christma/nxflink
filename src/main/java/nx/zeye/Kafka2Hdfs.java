package com.nx.pro.zeye.lesson01;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class Kafka2Hdfs {

  private static Logger LOG = LoggerFactory.getLogger(Kafka2Hdfs.class);

  public static void main(String[] args) throws Exception {
    //    if (args.length != 3) {
    //      LOG.error(
    //          "kafka(server01:9092), hdfs(hdfs://cluster01/data/), flink(parallelism=2) must be
    // exist.");
    //      System.exit(1);
    //    }

    StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

    String bootStrapServer = "192.168.208.51:9092";
    String hdfsPath = "hdfs://192.168.208.51:9000/liang/flinkdats";
    int parallelism = 1;

    env.enableCheckpointing(5000);
    env.setParallelism(parallelism);
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    DataStreamSource<String> source =
        env.addSource(
            new FlinkKafkaConsumer010<>(
                "nxflink", new SimpleStringSchema(), configByKafkaServer(bootStrapServer)));
    source.print();

    StreamingFileSink<String> sink =
        StreamingFileSink.forRowFormat(new Path(hdfsPath), new SimpleStringEncoder<String>("UTF-8"))
            .withRollingPolicy(
                DefaultRollingPolicy.builder()
                    .withRolloverInterval(TimeUnit.SECONDS.toSeconds(20))
                    .withInactivityInterval(TimeUnit.SECONDS.toSeconds(10))
                    .withMaxPartSize(1024)
                    .build())
            .build();

    source.addSink(sink);

    env.execute("Kafka2Hdfs");
  }

  private static Properties configByKafkaServer(String bootStrapServer) {
    Properties props = new Properties();
    props.setProperty("bootstrap.servers", bootStrapServer);
    props.setProperty("group.id", "nxflink_group");
    props.put("enable.auto.commit", "true");
    props.put("auto.commit.interval.ms", "1000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    return props;
  }
}
