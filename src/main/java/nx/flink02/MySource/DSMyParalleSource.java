package nx.flink02.MySource;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

//自定义多并行度数据源
public class DSMyParalleSource implements ParallelSourceFunction<Long> {
    private long number = 1L;
    private boolean isRunning = true;

    @Override
    public void run(SourceContext<Long> sourceContext) throws Exception {
        while (isRunning) {
            synchronized (this){
                sourceContext.collect(number);
            }
            number++;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
