package nx.UV;

import com.sun.scenario.effect.Bloom;
import nx.utils.Utils;
import nx.zeye.UserBehavior;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.swing.Action;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.sql.Timestamp;

public class UVByBloom {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.readTextFile(Utils.USER_BEHAVIOR_PATH)
                .map(new UserParser())
                .assignTimestampsAndWatermarks(new BloomEventTimeExtractor())
                .filter(userBehaviors -> userBehaviors.behavior.equalsIgnoreCase("P"))
                .map(new getKeyMapFunction())
                .keyBy(bk -> bk.f0)
                .timeWindow(Time.hours(1))
                .trigger(new UVTrigger())
                .process(new UvCountWithBlooms())
                .print();


        env.execute("UVByBloom");

    }

    private static class UserParser implements MapFunction<String, UserBehaviors> {
        @Override
        public UserBehaviors map(String line) throws Exception {
            String[] fields = line.split(",");
            return new UserBehaviors(Long.parseLong(fields[0].trim()),
                    Long.parseLong(fields[1].trim()),
                    Long.parseLong(fields[2].trim()),
                    fields[3].trim(),
                    Long.parseLong(fields[4].trim()),
                    fields[5].trim()
            );
        }
    }


    public static class UserBehaviors {
        private Long userId;
        private Long productId;
        private Long categoryId;
        private String behavior;
        private Long timeStamp;
        private String sessionId;

        public UserBehaviors() {

        }

        public UserBehaviors(Long userId, Long productId,
                             Long categoryId,
                             String behavior,
                             Long timeStamp,
                             String sessionId) {
            this.userId = userId;
            this.productId = productId;
            this.categoryId = categoryId;
            this.behavior = behavior;
            this.timeStamp = timeStamp;
            this.sessionId = sessionId;
        }

        @Override
        public String toString() {
            return "UserBehavior{" +
                    "userId=" + userId +
                    ", productId=" + productId +
                    ", categoryId=" + categoryId +
                    ", behavior='" + behavior + '\'' +
                    ", timeStamp=" + timeStamp +
                    ", sessionId='" + sessionId + '\'' +
                    '}';
        }

        public Long getUserId() {
            return userId;
        }

        public void setUserId(Long userId) {
            this.userId = userId;
        }

        public Long getProductId() {
            return productId;
        }

        public void setProductId(Long productId) {
            this.productId = productId;
        }

        public Long getCategoryId() {
            return categoryId;
        }

        public void setCategoryId(Long categoryId) {
            this.categoryId = categoryId;
        }

        public String getBehavior() {
            return behavior;
        }

        public void setBehavior(String behavior) {
            this.behavior = behavior;
        }

        public Long getTimeStamp() {
            return timeStamp;
        }

        public void setTimeStamp(Long timeStamp) {
            this.timeStamp = timeStamp;
        }

        public String getSessionId() {
            return sessionId;
        }

        public void setSessionId(String sessionId) {
            this.sessionId = sessionId;
        }

    }

    private static class BloomEventTimeExtractor implements AssignerWithPeriodicWatermarks<UserBehaviors> {

        private long currentEventTime = 0L;
        private int maxTimeOut = 10;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentEventTime - maxTimeOut);
        }

        @Override
        public long extractTimestamp(UserBehaviors userBehaviors, long l) {

            long timeStamp = userBehaviors.timeStamp * 1000;
            currentEventTime = Math.max(timeStamp, currentEventTime);
            return timeStamp;
        }
    }

    private static class getKeyMapFunction implements MapFunction<UserBehaviors, Tuple2<String, Long>> {
        @Override
        public Tuple2<String, Long> map(UserBehaviors userBehaviors) throws Exception {
            return Tuple2.of("key", userBehaviors.userId);
        }
    }

    private static class UVTrigger extends Trigger<Tuple2<String, Long>, TimeWindow> {
        @Override
        public TriggerResult onElement(Tuple2<String, Long> stringLongTuple2, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.FIRE_AND_PURGE;
        }

        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return null;
        }

        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            return null;
        }

        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

        }
    }


    public static class Bloom implements Serializable {
        private long cap;

        public long getCap() {
            if (cap > 0) {
                return cap;
            } else {
                return 1 << 28;
            }
        }

        public void setCap(long cap) {
            this.cap = cap;
        }

        public Bloom(long cap) {
            this.cap = cap;
        }

        public Bloom() {

        }

        public long hash(String value, int seed) {
            long result = 0L;
            for (int i = 0; i < value.length(); i++) {
                result += result * seed + value.charAt(i);
            }
            //他们之间进行&运算结果一定在位图之间
            return result & (cap - 1);//0后面28个1
        }

    }

    private static class UvCountWithBlooms extends ProcessWindowFunction<Tuple2<String, Long>, UVCount.UvInfo, String, TimeWindow> {


        //初始化自己的布隆过滤器（32M）
        public Bloom bloom = new Bloom(1 << 28);
        //初始化了一个redis的工具类。
        RedisUtils redisUtils = new RedisUtils();

        @Override
        public void process(String s, Context context, Iterable<Tuple2<String, Long>> elements, Collector<UVCount.UvInfo> collector) throws Exception {

            String redisKey = new Timestamp(context.window().getEnd()).toString();
            long count = 0l;

            String rCount = redisUtils.jedis.hget("count", redisKey);
            if (rCount != null) {
                count = Long.parseLong(rCount);
            }

            String userId = elements.iterator().next().f1.toString();

            long offset = bloom.hash(userId, 77);

            Boolean ifExist = redisUtils.jedis.getbit(redisKey, offset);
            if (!ifExist) {
                redisUtils.jedis.setbit(redisKey, offset, true);
                redisUtils.jedis.hset("count", redisKey, count + 1 + "");
                collector.collect(new UVCount.UvInfo(new Timestamp(context.window().getEnd()).toString(), count + 1));
            } else {
                collector.collect(new UVCount.UvInfo(new Timestamp(context.window().getEnd()).toString(), count));
            }

        }
    }
}
