package nx.zeye;

import nx.utils.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

public class TopNPage {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.readTextFile(Utils.APACHE_LOG_PAH) //读取数据
                .map(new ParseZeyeLog());
        env.execute("TopNPage");
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
