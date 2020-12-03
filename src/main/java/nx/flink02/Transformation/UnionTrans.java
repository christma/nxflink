package nx.flink02.Transformation;

import nx.flink02.MySource.MyNoParalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class UnionTrans {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Long> text1 = env.addSource(new MyNoParalleSource()).setParallelism(1);

        DataStreamSource<Long> text2 = env.addSource(new MyNoParalleSource()).setParallelism(1);


        DataStream<Long> test = text1.union(text2);


        SingleOutputStreamOperator<Long> num = test.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long aLong) throws Exception {
                System.out.println("原始接受到数据： " + aLong);
                return aLong;
            }
        });
        SingleOutputStreamOperator<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);

        sum.print().setParallelism(1);

        env.execute("UnionTrans");
    }
}
