package com.my.ch09;

import com.my.ch5.ClickSource;
import com.my.ch5.Event;
import com.my.ch5.SinkFileTest;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

public class BufferingSinkExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointTimeout(1000L);
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //前一个检查点处理完成后到后一个检查点的时间
        checkpointConfig.setMinPauseBetweenCheckpoints(500L);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        stream.print("input");
         //批量缓存输出
        stream.addSink(new BufferingSink(10));

        env.execute();

    }

    public static  class BufferingSink implements SinkFunction<Event>, CheckpointedFunction {

        //定义当天类的属性
        private final int threshold;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferElement = new ArrayList<>();
        }

        private List<Event> bufferElement;

        //需要去定义一个算子的状态

        private ListState<Event> checkpointstate;

        @Override
        public void invoke(Event value, Context context) throws Exception {
            bufferElement.add(value);//缓存列表
            //判断达到阈值，就批量写入
            if (bufferElement.size() ==threshold){
                //打印到控制台模拟写入外部系统
                for (Event element:bufferElement){
                    System.out.println("shuchu:"+element);
                }
                System.out.println("------输入完毕-------");
                bufferElement.clear();

            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            //处理之前，清空状态
            checkpointstate.clear();
            //对状态进行持久化，复制缓存的列表到列表状态
            for (Event element:bufferElement){
                checkpointstate.add(element);
            }


        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            //定义算子状态
            ListStateDescriptor<Event> descriptor = new ListStateDescriptor<>("buffered-element", Event.class);

            //上下文中获取状态
            checkpointstate = context.getOperatorStateStore().getListState(descriptor);

            //如果从故障恢复，需要将ListState中的所有元素复制到列表中
            if(context.isRestored()){

                for (Event element:checkpointstate.get()){
                    bufferElement.add(element);
                }
            }

        }
    }
}
