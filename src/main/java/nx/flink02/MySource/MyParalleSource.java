package nx.flink02.MySource;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author 020972
 */
public class MyParalleSource {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> dataStream = env.addSource(new DSMyParalleSource()).setParallelism(2);

        SingleOutputStreamOperator<Long> mapStream = dataStream.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long aLong) throws Exception {

                System.out.println("接收道德数据 " + aLong);
                return aLong;
            }
        });

        SingleOutputStreamOperator<Long> filter = mapStream.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long aLong) throws Exception {

                return aLong % 2 == 0;
            }
        });

        filter.print().setParallelism(1);


        env.execute("MyParalleSource");

    }
}
