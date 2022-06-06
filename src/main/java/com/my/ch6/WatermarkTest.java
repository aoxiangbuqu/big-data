package com.my.ch6;

import com.my.ch5.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;

import java.time.Duration;

public class WatermarkTest {
    public static void main(String[] args)  throws  Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //默认调用WatermarkGenerator中的周期性方法onPeriodicEmit 默认是200L
        env.getConfig().setAutoWatermarkInterval(100);
        DataStream<Event> stream = env.fromElements(new Event("Mary", "/home", 1000L),
                (new Event("Bob", "/cart", 2000L)),
                (new Event("Alice", "/prod?id=100", 3000L)),
                (new Event("Bob", "/prod?id=10", 3200L)),
                (new Event("Bob", "/prod?id=20", 3500L)),
                (new Event("Alice", "/prod?id=12", 31000L))

        )  //有序流的watermarks生成
                //.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
                //        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                //            @Override
                //            public long extractTimestamp(Event element, long recordTimestamp) {
                //                return element.timestamp;
                //            }
                //        })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5L))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        })
                );

        
            env.execute();
    }

}
