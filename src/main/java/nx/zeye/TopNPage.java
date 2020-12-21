package nx.zeye;

import nx.utils.Utils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import sun.rmi.runtime.Log;

import javax.annotation.Nullable;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TopNPage {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.readTextFile(Utils.APACHE_LOG_PAH) //读取数据
                .map(new ParseZeyeLog())// 解析数据
                .assignTimestampsAndWatermarks(new ZeyeHotPageEventTimeExtractor()) // 设置水位
                .keyBy(logEvent -> logEvent.url) // 根据url 分组
                .timeWindow(Time.seconds(10), Time.seconds(5)) // 每5秒统计 10秒的数据  滑动窗口
                .aggregate(new ZeyePageCountAgg(), new ZeyePageWindowResult())
                .keyBy(urlView -> urlView.WindowTime)
                .process(new ZeyeTopNPage(3))
                .print();
        env.execute("TopNPage");

    }

    public static class ZeyeTopNPage extends KeyedProcessFunction<Long, ZeyeUrlView, String> {

        private int TopN = 0;

        public ZeyeTopNPage(int topN) {
            TopN = topN;
        }

        public long getTopN() {
            return TopN;
        }

        public void setTopN(int topN) {
            TopN = topN;
        }

        public MapState<String, Long> urlState;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>(
                    "urlState",
                    String.class,
                    Long.class
            );

            urlState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(ZeyeUrlView urlView, Context context, Collector<String> collector) throws Exception {

            urlState.put(urlView.url, urlView.count);
            context.timerService().registerEventTimeTimer(urlView.WindowTime);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            List<ZeyeUrlView> urlViewArrayList = new ArrayList<>();

            ArrayList<String> keys = Lists.newArrayList(urlState.keys());
            for (String url : keys) {
                Long count = urlState.get(url);
                urlViewArrayList.add(new ZeyeUrlView(url, new Timestamp(timestamp - 1).getTime(), count));
            }

            Collections.sort(urlViewArrayList);
            List<ZeyeUrlView> topN = urlViewArrayList.subList(0, this.TopN);
            for (ZeyeUrlView view : topN) {
                System.out.println(view);
            }
            System.out.println("==================");
        }
    }

    public static class ZeyePageWindowResult implements WindowFunction<Long, ZeyeUrlView, String, TimeWindow> {

        @Override
        public void apply(String key, TimeWindow timeWindow, Iterable<Long> iterable, Collector<ZeyeUrlView> collector) throws Exception {

            collector.collect(new ZeyeUrlView(key, timeWindow.getEnd(), iterable.iterator().next()));
        }
    }

    public static class ZeyePageCountAgg implements AggregateFunction<ZeyeApacheLogEvent, Long, Long> {

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(ZeyeApacheLogEvent zeyeApacheLogEvent, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc1, Long acc2) {
            return acc1 + acc2;
        }
    }

    /**
     * 指定事件时间段
     */
    public static class ZeyeHotPageEventTimeExtractor implements AssignerWithPeriodicWatermarks<ZeyeApacheLogEvent> {

        private long currentMaxEventTime = 0L;
        private long maxOutOfOrderness = 10000L;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxEventTime - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(ZeyeApacheLogEvent zeyeApacheLogEvent, long l) {
            Long eventTime = zeyeApacheLogEvent.getEventTime();
            currentMaxEventTime = Math.max(eventTime, currentMaxEventTime);
            return eventTime;
        }
    }

    public static class ParseZeyeLog implements MapFunction<String, ZeyeApacheLogEvent> {

        @Override
        public ZeyeApacheLogEvent map(String lines) throws Exception {
            String[] fields = lines.split(" ");
            SimpleDateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
            long timeStamp = dateFormat.parse(fields[3].trim()).getTime();

            return new ZeyeApacheLogEvent(fields[0].trim(), fields[1].trim(),
                    timeStamp, fields[5].trim(), fields[6].trim());
        }
    }

    public static class ZeyeUrlView implements Comparable<ZeyeUrlView> {
        private String url;
        private Long WindowTime;
        private Long count;

        public ZeyeUrlView() {

        }

        public ZeyeUrlView(String url, Long windowTime, Long count) {
            this.url = url;
            WindowTime = windowTime;
            this.count = count;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        public Long getWindowTime() {
            return WindowTime;
        }

        public void setWindowTime(Long windowTime) {
            WindowTime = windowTime;
        }

        public Long getCount() {
            return count;
        }

        public void setCount(Long count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "ZeyeUrlView{" +
                    "url='" + url + '\'' +
                    ", WindowTime=" + WindowTime +
                    ", count=" + count +
                    '}';
        }

        @Override
        public int compareTo(ZeyeUrlView urlView) {
            return (this.count > urlView.count) ? -1 : (this.count == urlView.count ? 0 : 1);
        }
    }

    public static class ZeyeApacheLogEvent {
        private String ip;
        private String userId;
        private Long eventTime;
        private String method;
        private String url;

        public ZeyeApacheLogEvent() {

        }

        public ZeyeApacheLogEvent(String ip, String userId, Long eventTime, String method, String url) {
            this.ip = ip;
            this.userId = userId;
            this.eventTime = eventTime;
            this.method = method;
            this.url = url;
        }

        public String getIp() {
            return ip;
        }

        public void setIp(String ip) {
            this.ip = ip;
        }

        public String getUserId() {
            return userId;
        }

        public void setUserId(String userId) {
            this.userId = userId;
        }

        public Long getEventTime() {
            return eventTime;
        }

        public void setEventTime(Long eventTime) {
            this.eventTime = eventTime;
        }

        public String getMethod() {
            return method;
        }

        public void setMethod(String method) {
            this.method = method;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }

        @Override
        public String toString() {
            return "ZeyeApacheLogEvent{" +
                    "ip='" + ip + '\'' +
                    ", userId='" + userId + '\'' +
                    ", eventTime=" + eventTime +
                    ", method='" + method + '\'' +
                    ", url='" + url + '\'' +
                    '}';
        }
    }
}
