package nx.flink.demo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class ObjWordCount {
    public static void main(String[] args) throws Exception {
        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //获取数据源
        DataStreamSource<String> dataStream = env.socketTextStream("192.168.208.51", 9999);

        //数据处理
        SingleOutputStreamOperator<WordAndCount> wc = dataStream.flatMap(new FlatMapFunction<String, WordAndCount>() {
            @Override
            public void flatMap(String line, Collector<WordAndCount> collector) throws Exception {
                String[] fields = line.split(",");
                for (String word : fields) {
                    collector.collect(new WordAndCount(word, 1));
                }
            }
        }).keyBy("word").sum("count");

        //数据输出
        wc.print();

        //启动任务
        env.execute("ObjWordCount");
    }

    public static class WordAndCount {
        private String word;
        private Integer count;


        public WordAndCount(){

        }
        public WordAndCount(String word, Integer count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordAndCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}



