package nx.flink03;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import scala.Int;

import java.util.ArrayList;
import java.util.List;

public class CustomSink implements SinkFunction<Tuple2<String, Integer>>, CheckpointedFunction {
    private List<Tuple2<String, Integer>> bufferElements;
    private int threshold;
    private ListState<Tuple2<String, Integer>> checkpointState;

    public CustomSink(int threshold) {
        this.threshold = threshold;
        this.bufferElements = new ArrayList<>();
    }

    //用于将内存中数据保存到状态中
    @Override
    public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
        checkpointState.clear();
        for (Tuple2<String, Integer> ele : bufferElements) {
            checkpointState.add(ele);
        }
    }

    // 用于在程序挥发的时候从状态中恢复数据到内存
    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        ListStateDescriptor<Tuple2<String, Integer>> descriptor = new ListStateDescriptor<>(
                "bufferd-elements",
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {
                })
        );
        checkpointState = context.getOperatorStateStore().getListState(descriptor);
        if (context.isRestored()) {
            for (Tuple2<String, Integer> ele : checkpointState.get()) {
                bufferElements.add(ele);
            }
        }
    }

    @Override
    public void invoke(Tuple2<String, Integer> value) throws Exception {

    }

    @Override
    public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
        bufferElements.add(value);
        if (bufferElements.size() == threshold) {
            System.out.println("自定义格式 ： " + bufferElements);
            bufferElements.clear();
        }
    }
}
