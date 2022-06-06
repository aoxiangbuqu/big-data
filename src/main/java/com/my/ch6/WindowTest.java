package com.my.ch6;

import com.my.ch5.ClickSource;
import com.my.ch5.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

public class WindowTest {
    private KeySelector keySelector;

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //默认是水位线周期 周是200L
        env.getConfig().setAutoWatermarkInterval(100);
//        DataStream<Event> stream = env.fromElements(new Event("Mary", "/home", 1000L),
//                (new Event("Bob", "/cart", 2000L)),
//                (new Event("Alice", "/prod?id=100", 3000L)),
//                (new Event("Bob", "/prod?id=10", 3200L)),
//                (new Event("Bob", "/prod?id=20", 3500L)),
//                (new Event("Alice", "/prod?id=12", 31000L))
//
//        )

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        stream.map(new MapFunction<Event,Tuple2<String,Long>>() {
            @Override
            public Tuple2<String, Long> map(Event value) throws Exception {
                return Tuple2.of(value.user,1L);
            }
        }).keyBy(data ->data.f0)
                 //滑动事件事件窗口
                //.window(SlidingEventTimeWindows.of(Time.hours(1),Time.minutes(5)));
                 //滚动事件窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                //.window(EventTimeSessionWindows.withGap(Time.milliseconds(10))
        .reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of(value1.f0,value1.f1+value2.f1);
            }
        }).print();

        env.execute();
    }

}
