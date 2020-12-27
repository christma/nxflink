package nx.order;

import nx.flink04.TimeWindowWC;
import nx.utils.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Iterator;

public class OrderAmount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 9999);


        env.readTextFile(Utils.ORDER_TEST_PATH)
                .map(new ParserOrderLog())
                .assignTimestampsAndWatermarks(new ExtratorOrderEventTime())
                .keyBy(key -> key.f0)
                .window(TumblingEventTimeWindows.of(Time.days(1), Time.hours(16)))
                .trigger(ContinuousEventTimeTrigger.of(Time.seconds(5)))
                .evictor(TimeEvictor.of(Time.seconds(0), true))
                .process(new CountAmountProcess())
                .print();


        env.execute("OrderAmount");

    }

    private static class ParserOrderLog implements MapFunction<String, Tuple3<String, String, Double>> {

        @Override
        public Tuple3<String, String, Double> map(String line) throws Exception {
            String[] fields = line.split("\t");
            String date = fields[6].trim();
            double amount = Double.parseDouble(fields[1].trim());
            String day = date.split(" ")[0];


            return Tuple3.of(day, date, amount);
        }
    }

    private static class ExtratorOrderEventTime implements AssignerWithPeriodicWatermarks<Tuple3<String, String, Double>> {

        public long currentMaxEventTime = 0L;
        public int maxOutofOrderness = 10000;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxEventTime - maxOutofOrderness);
        }

        @Override
        public long extractTimestamp(Tuple3<String, String, Double> tuple3, long l) {
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            try {
                long time = dateFormat.parse(tuple3.f1).getTime();
                currentMaxEventTime = Math.max(time, currentMaxEventTime);
                return time;
            } catch (ParseException e) {
                e.printStackTrace();
            }
            return 0;
        }
    }


    private static class CountAmountProcess extends ProcessWindowFunction<Tuple3<String, String, Double>, Double, String, TimeWindow> {

        private ValueState<Double> countAmountState;

        @Override
        public void open(Configuration parameters) throws Exception {

            ValueStateDescriptor<Double> descriptor = new ValueStateDescriptor<>(
                    "countAmountState",
                    Double.class
            );

            countAmountState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void process(String s, Context context, Iterable<Tuple3<String, String, Double>> iterable, Collector<Double> collector) throws Exception {

            Double countAmount = countAmountState.value();

            if (countAmount == null) {
                countAmount = 0.0;
            }

            Iterator<Tuple3<String, String, Double>> iterator = iterable.iterator();

            while (iterator.hasNext()) {
                countAmount += iterator.next().f2;
            }
            countAmountState.update(countAmount);
            collector.collect(countAmount);
        }
    }
}
