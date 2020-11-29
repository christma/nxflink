package nx.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {

        // 获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据源
        DataStream<String> dataStream = env.socketTextStream("localhost", 9999);

        //数据处理

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {

                String[] split = line.split(",");
                for (String word : split) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }

            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCount = wordAndOne.keyBy(0).sum(1);
        wordCount.print();

        env.execute("word count demo");


    }
}
