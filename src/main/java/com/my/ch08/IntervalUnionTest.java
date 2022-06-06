package com.my.ch08;

import com.my.ch5.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class IntervalUnionTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> clickStream = env.fromElements(
                new Event("Mary", "/home", 10000L),
                (new Event("Bob", "/cart", 2000L)),
                (new Event("Alice", "/prod?id=100", 30000L)),
                (new Event("Bob", "/prod?id=10", 32000L)),
                (new Event("Bob", "/prod?id=20", 75000L)),
                (new Event("Alice", "/prod?id=12", 31000L))

        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                           @Override
                                           public long extractTimestamp(Event element, long recordTimestamp) {
                                               return element.timestamp;
                                           }
                                       }
                ));

        SingleOutputStreamOperator<Tuple2<String, Long>> orderStream = env.fromElements(
                Tuple2.of("Mary", 30000L),
                Tuple2.of("Bob", 40000L),
                Tuple2.of("Alice", 4000L),
                Tuple2.of("Bob", 6000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ZERO)
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                        return element.f1;
                    }
                }));
       orderStream.keyBy(data -> data.f0)
               .intervalJoin(clickStream.keyBy(data -> data.user))
               .between(Time.seconds(-5),Time.seconds(10))
               .process(new ProcessJoinFunction<Tuple2<String, Long>, Event, String>() {
                   @Override
                   public void processElement(Tuple2<String, Long> left, Event right, Context ctx, Collector<String> out) throws Exception {
                       out.collect(right+ "==>"+left);
                   }
               }).print();

    env.execute();
    }
}
