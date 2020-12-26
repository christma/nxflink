package nx.adClickCount;

import nx.utils.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;

public class AdClockCount {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        SingleOutputStreamOperator<AdClickEvent> adEventStream = env.readTextFile(Utils.CLICK_LOG_PATH)
                .map(new parseAdClickLog())
                .assignTimestampsAndWatermarks(new AdClickEventTimeExtractor());


        adEventStream.keyBy(new getKey()).process(new FilterBlackListUser(100))


        env.execute("AdClockCount");
    }

    private static class parseAdClickLog implements MapFunction<String, AdClickEvent> {

        @Override
        public AdClickEvent map(String line) throws Exception {

            String[] field = line.split(",");
            return null;
        }
    }

    public static class AdClickEvent {
        private Long userId; //用户ID
        private Long adId; //广告ID
        private String province; //省份
        private String city; //城市
        private Long timestamp; //点击广告的事件时间

        public AdClickEvent() {

        }

        @Override
        public String toString() {
            return "AdClickEvent{" +
                    "userId=" + userId +
                    ", adId=" + adId +
                    ", province='" + province + '\'' +
                    ", city='" + city + '\'' +
                    ", timestamp=" + timestamp +
                    '}';
        }

        public AdClickEvent(Long userId, Long adId, String province,
                            String city, Long timestamp) {
            this.userId = userId;
            this.adId = adId;
            this.province = province;
            this.city = city;
            this.timestamp = timestamp;
        }

        public Long getUserId() {
            return userId;
        }

        public void setUserId(Long userId) {
            this.userId = userId;
        }

        public Long getAdId() {
            return adId;
        }

        public void setAdId(Long adId) {
            this.adId = adId;
        }

        public String getProvince() {
            return province;
        }

        public void setProvince(String province) {
            this.province = province;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        public Long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
        }
    }

    private static class AdClickEventTimeExtractor implements AssignerWithPeriodicWatermarks<AdClickEvent> {
        private long currentMaxEventTime = 0L;
        private long maxOutOfOrderness = 10L;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark((currentMaxEventTime - maxOutOfOrderness) * 1000);
        }

        @Override
        public long extractTimestamp(AdClickEvent adClickEvent, long l) {
            Long timestamp = adClickEvent.timestamp * 1000;

            currentMaxEventTime = Math.max(currentMaxEventTime, adClickEvent.timestamp);

            return timestamp;
        }
    }

    private static class getKey implements KeySelector<AdClickEvent, Tuple2<Long, Long>> {
        @Override
        public Tuple2<Long, Long> getKey(AdClickEvent adClickEvent) throws Exception {
            return Tuple2.of(adClickEvent.getUserId(), adClickEvent.adId);
        }
    }

    private static class FilterBlackListUser extends KeyedProcessFunction<Tuple2<Long, Long>, AdClickEvent, AdClickEvent> {
        public int maxCount;
        public ValueState<Long> countState;
        public ValueState<Boolean> isSetBlackList;
        public ValueState<Long> resetTimeer;


        @Override
        public void open(Configuration parameters) throws Exception {

            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<>("count-state", Long.class);
            countState = getRuntimeContext().getState(descriptor);

            ValueStateDescriptor<Boolean> descriptor1 = new ValueStateDescriptor<>("issent-state", Boolean.class);
            new ValueStateDescriptor<>("issent-state", Boolean.class);

            isSetBlackList = getRuntimeContext().getState(descriptor1);

            ValueStateDescriptor<Long> descriptor2 = new ValueStateDescriptor<>("resettime-state", Long.class);

            resetTimeer = getRuntimeContext().getState(descriptor2);


        }

        public FilterBlackListUser(int maxCount) {
            this.maxCount = maxCount;
        }

        @Override
        public void processElement(AdClickEvent adClickEvent, Context context, Collector<AdClickEvent> collector) throws Exception {

        }
    }
}
