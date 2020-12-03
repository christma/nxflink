package nx.flink02.MySource;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

//自定义单并行度数据源
public class MyNoParalleSource implements SourceFunction<Long> {

    private long number = 1L;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> sc) throws Exception {

        while (isRunning) {
            sc.collect(number);
            number++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
