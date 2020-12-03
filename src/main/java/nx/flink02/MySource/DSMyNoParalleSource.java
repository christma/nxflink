package nx.flink02.MySource;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DSMyNoParalleSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> source = env.addSource(new MyNoParalleSource()).setParallelism(1);

        SingleOutputStreamOperator<Long> dataStream = source.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long aLong) throws Exception {

                System.out.println("接受到的数据为： " + aLong);
                return aLong;
            }
        });

        SingleOutputStreamOperator<Long> f = dataStream.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long aLong) throws Exception {
                return aLong % 2 == 0;
            }
        });

        f.print().setParallelism(1);


        env.execute("DSMyNoParalleSource");
    }
}
