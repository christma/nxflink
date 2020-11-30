package nx.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class LeaveWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool tool = ParameterTool.fromArgs(args);

        String hostname = tool.get("hostname");
        int port = tool.getInt("port");

        DataStreamSource<String> dataStream = env.socketTextStream(hostname, port);

        SingleOutputStreamOperator<ObjWordCount.WordAndCount> wc = dataStream.flatMap(new SplitWord()).keyBy("word").sum("count");

        wc.print();


        env.execute("LeaveWordCount");


    }

    private static class SplitWord implements FlatMapFunction<String, ObjWordCount.WordAndCount> {
        @Override
        public void flatMap(String lines, Collector<ObjWordCount.WordAndCount> collector) throws Exception {
            if (lines.isEmpty() | "".equals(lines)) {
                return;
            }
            String[] feilds = lines.split(",");
            for (String word : feilds) {
                collector.collect(new ObjWordCount.WordAndCount(word, 1));
            }
        }
    }
}
