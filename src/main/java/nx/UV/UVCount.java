package nx.UV;

import nx.utils.Utils;
import org.apache.commons.math3.stat.descriptive.rank.Max;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class UVCount {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.readTextFile(Utils.USER_BEHAVIOR_PATH)
                .map(new ParserUserLog())
                .assignTimestampsAndWatermarks(new EventTimeExtractor())
                .filter(userBehavior -> userBehavior.behavior.equalsIgnoreCase("P"))
                .timeWindowAll(Time.hours(1))// 滚动窗口
                .apply(new UVCountByWindow())
                .print();


        env.execute("UVCounet");


    }

    private static class ParserUserLog implements MapFunction<String, UserBehavior> {
        @Override
        public UserBehavior map(String line) throws Exception {

            String[] fields = line.split(",");

            return new UserBehavior(Long.parseLong(fields[0].trim()),
                    Long.parseLong(fields[1].trim()),
                    Long.parseLong(fields[2].trim()),
                    fields[3].trim(),
                    Long.parseLong(fields[4].trim()),
                    fields[5].trim());
        }
    }

    private static class UserBehavior {
        private Long userId;
        private Long productId;
        private Long categoryId;
        private String behavior;
        private Long timeStamp;
        private String sessionId;

        public UserBehavior() {

        }

        public UserBehavior(Long userId, Long productId,
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

    private static class EventTimeExtractor implements AssignerWithPeriodicWatermarks<UserBehavior> {

        private long currentTime = 0L;
        private int maxOutOftime = 10;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentTime - maxOutOftime);
        }

        @Override
        public long extractTimestamp(UserBehavior userBehavior, long l) {

            Long timeStamp = userBehavior.timeStamp * 1000;
            currentTime = Math.max(currentTime, timeStamp);

            return timeStamp;
        }
    }

    private static class UVCountByWindow implements AllWindowFunction<UserBehavior, UvInfo, TimeWindow> {


        @Override
        public void apply(TimeWindow timeWindow, Iterable<UserBehavior> iterable, Collector<UvInfo> collector) throws Exception {
            Set<Long> userIds = new HashSet<Long>();

            Iterator<UserBehavior> behaviorIterator = iterable.iterator();
            while (behaviorIterator.hasNext()) {
                userIds.add(behaviorIterator.next().userId);
            }
            collector.collect(new UvInfo(new Timestamp(timeWindow.getEnd()) + "",
                    Long.parseLong(userIds.size() + "")));
        }
    }

    public static class UvInfo {
        private String windowEnd;
        private Long uvCount;

        public UvInfo() {

        }

        @Override
        public String toString() {
            return "UvInfo{" +
                    "windowEnd='" + windowEnd + '\'' +
                    ", uvCount=" + uvCount +
                    '}';
        }

        public UvInfo(String windowEnd, Long uvCount) {
            this.windowEnd = windowEnd;
            this.uvCount = uvCount;
        }

        public String getWindowEnd() {
            return windowEnd;
        }

        public void setWindowEnd(String windowEnd) {
            this.windowEnd = windowEnd;
        }

        public Long getUvCount() {
            return uvCount;
        }

        public void setUvCount(Long uvCount) {
            this.uvCount = uvCount;
        }
    }

}
