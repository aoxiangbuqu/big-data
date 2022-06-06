package com.my.ch08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class BillCheckExample {

    public static void main(String[] args)  throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //来自app的支付日志
        SingleOutputStreamOperator<Tuple3<String,String,Long>> appStream = env.fromElements(
                Tuple3.of("order-1","app",1000L),
                Tuple3.of("order-2","app",2000L),
                Tuple3.of("order-3","app",3500L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String,String,Long>>forBoundedOutOfOrderness(Duration.ZERO)
        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, String, Long>>() {
            @Override
            public long extractTimestamp(Tuple3<String, String, Long> element, long recordTimestamp) {
                return element.f2;
            }
        }));

        //来自第三方支付平台的支付日志
        SingleOutputStreamOperator<Tuple4<String,String,String,Long>> thirdStream = env.fromElements(
                Tuple4.of("order-1","third","success",3000L),
                Tuple4.of("order-2","third","success",4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String,String,String,Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple4<String,String,String,Long>>() {
                    @Override
                    public long extractTimestamp(Tuple4<String,String,String,Long> element, long recordTimestamp) {
                        return element.f3;
                    }
                }));
        //检测同一支付单在两条流中是否匹配，不匹配就报警
        appStream.connect(thirdStream)
                .keyBy(data ->data.f0,data ->data.f0)
                .process(new orderMatchResult())
                .print();
        env.execute();
    }
    //自定义实现CoProcessFunction
    public static class orderMatchResult extends CoProcessFunction<Tuple3<String,String,Long>,Tuple4<String,String,String,Long>,String>{
        //定义状态变量，用来保存已经到达的事件
        private ValueState<Tuple3<String, String, Long>> appEventState;
        private ValueState<Tuple4<String, String, String, Long>> thirdEventState;

        @Override
        public void open(Configuration parameters) throws Exception {
            appEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple3<String, String, Long>>("app-event", Types.TUPLE(Types.STRING,Types.STRING,Types.LONG))
            );

            thirdEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple4<String, String, String, Long>>("app-event", Types.TUPLE(Types.STRING,Types.STRING,Types.STRING,Types.LONG))
            );
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
           //来的是app event，看另一条流中事件是否来过

            if (thirdEventState.value() != null ){
                out.collect("对账成功："+value+" "+thirdEventState.value());
                //清空状态
                thirdEventState.clear();
            }
            else {
                //更新状态
                appEventState.update(value);
                //定义注册一个5s定时器，开始等待另一条流的事件
                ctx.timerService().registerEventTimeTimer(value.f2+5000L);
            }

        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, Context ctx, Collector<String> out) throws Exception {
            if (appEventState.value() != null ){
                out.collect("对账成功："+appEventState.value()+" "+value);
                //清空
               appEventState.clear();
            }
            else {
                //更新状态
                thirdEventState .update(value);
                //定义注册一个5s定时器，开始等待另一条流的事件
                ctx.timerService().registerEventTimeTimer(value.f3);
            }

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            //定时器触发，判断状态，如果某个状态不为空，说明另一条流事件没来
            if (appEventState.value() != null){
                out.collect("对账失败："+appEventState.value()+" 第三方未到");

            }

            if (thirdEventState.value() != null){
                out.collect("对账失败："+thirdEventState.value()+" app信息未到");
            }

            appEventState.clear();
            thirdEventState.clear();
        }
    }
}
