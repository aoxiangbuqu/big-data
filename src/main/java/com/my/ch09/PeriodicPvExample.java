package com.my.ch09;

import com.my.ch5.ClickSource;
import com.my.ch5.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class PeriodicPvExample {

    public static void main(String[] args) throws  Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );
        stream.print("data");
        //统计每个用户的pv
        stream.keyBy(data -> data.user)
                .process(new PeriodicResult())
                .print();
        env.execute();

    }
    //实现自定义的KeyedProcessFunction
    public static class PeriodicResult extends KeyedProcessFunction<String,Event,String>{
       //定义状态，保存当天统计值，以及是否有定时器
        ValueState<Long> countState;
        ValueState<Long> timerTsState;

        @Override
        public void open(Configuration parameters) throws Exception {

            countState =getRuntimeContext().getState(new ValueStateDescriptor<Long>("count",Long.class));
            timerTsState =getRuntimeContext().getState(new ValueStateDescriptor<Long>("timer-ts",Long.class));

        }
        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            //没来一条数据更新对应的count值
            Long count = countState.value();
            countState.update(count == null?1:count+1);

            //注册定时器，如果没有注册过，才去注册
            if (timerTsState.value()==null){
                ctx.timerService().registerEventTimeTimer(value.timestamp+10*1000L);
                timerTsState.update(value.timestamp+10*1000L);
            }

        }
     //定时器触发，输出一次统计结果
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            out.collect(ctx.getCurrentKey()+"的pv"+countState.value());
            //清空状态
            timerTsState.clear();

            //ctx.timerService().registerEventTimeTimer(timestamp+10*1000L);
            //timerTsState.update(timestamp+10*1000L);
        }
    }
}
