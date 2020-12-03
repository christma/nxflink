package nx.flink02.Transformation;

import nx.flink.demo.WordCount;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/*

滑动窗口实现单词统计
数据源：socket
需求：每隔1秒计算近2秒单词出现的次数
 */
public class WindowWordCountJava {
    public static void main(String[] args) throws Exception {
        int port;
        try {
            ParameterTool tool = ParameterTool.fromArgs(args);
            port = tool.getInt("port");
        } catch (Exception e) {
            System.out.println("no port set, user default port 9988");
            port = 9988;
        }
        String hostname = "192.168.208.51";
        String delimiter = "\n";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream(hostname, port, delimiter);

        SingleOutputStreamOperator<WC> wcs = source.flatMap(new FlatMapFunction<String, WC>() {
            @Override
            public void flatMap(String line, Collector<WC> out) throws Exception {

                String[] fields = line.split("\t");
                for (String word : fields) {
                    out.collect(new WC(word, 1));
                }
            }
        }).keyBy("word").timeWindow(Time.seconds(2), Time.seconds(1)).sum("count");

        wcs.print().setParallelism(1);


        env.execute("WindowWordCountJava");

    }

    public static class WC {
        public String word;
        public int count;

        public WC() {

        }

        public WC(String word, int count) {
            this.word = word;
            this.count = count;
        }


        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }


        @Override
        public String toString() {
            return "WC{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}


