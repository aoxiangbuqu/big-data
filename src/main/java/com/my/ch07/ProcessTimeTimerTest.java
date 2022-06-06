package com.my.ch07;

import com.my.ch5.ClickSource;
import com.my.ch5.Event;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

public class ProcessTimeTimerTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource());

        stream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, String>() {
                    @Override
                    public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
                        Long currTs = ctx.timerService().currentProcessingTime();
                        out.collect(ctx.getCurrentKey()+"数据到达时间："+new Timestamp(currTs));
                        //注册1个10秒后的定时器

                        ctx.timerService().registerProcessingTimeTimer(currTs + 10*1000L);
                    }
                    //第一个参数就是定时器设置的触发时间
                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey()+"数据触发时间："+new Timestamp(timestamp));

                    }
                }).print();
        env.execute();
    }
}