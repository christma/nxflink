package nx.flink02.Transformation;

import nx.flink02.MySource.MyNoParalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class ConnectionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> source1 = env.addSource(new MyNoParalleSource()).setParallelism(1);

        DataStreamSource<Long> source2 = env.addSource(new MyNoParalleSource()).setParallelism(1);


        SingleOutputStreamOperator<String> nx_source = source2.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "nx_" + value;
            }
        });

        ConnectedStreams<Long, String> connect = source1.connect(nx_source);


        SingleOutputStreamOperator<Object> connectStream = connect.map(new CoMapFunction<Long, String, Object>() {
            @Override
            public Object map1(Long v) throws Exception {
                return v;
            }

            @Override
            public Object map2(String s) throws Exception {
                return s;
            }
        });

        connectStream.print().setParallelism(1);


        env.execute("ConnectionDemo");
    }
}
