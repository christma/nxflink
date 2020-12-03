package nx.flink02.collection;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class SSFromCollection {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> data = new ArrayList<String>();
        data.add("hadoop");
        data.add("flink");
        data.add("spark");


        DataStreamSource<String> dataStream = env.fromCollection(data);

        SingleOutputStreamOperator<String> addPreStream = dataStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String word) throws Exception {
                return "kafka_" + word;
            }
        });

        addPreStream.print().setParallelism(1);


        env.execute("SSFromCollection");
    }
}
