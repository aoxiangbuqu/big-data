package com.my.ch6;

import com.my.ch5.ClickSource;
import com.my.ch5.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.HashSet;


//开窗统计pv和uv 统计pv/uv 平均数
public class WindowAggregateTest_PvUv {
    public static void main(String[] args) throws Exception {


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
        stream.keyBy(data ->true)
                .window(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(2)))
                .aggregate(new AvgPv())
                .print();
        env.execute();
    }

    //自定义aggregation,用long保存pv 用hashSet做uv去重

    public static class AvgPv implements AggregateFunction<Event, Tuple2<Long, HashSet>,Double>{
        @Override
        public Tuple2<Long, HashSet> createAccumulator() {
            return Tuple2.of(0L,new HashSet());
        }

        @Override
        public Tuple2<Long, HashSet> add(Event value, Tuple2<Long, HashSet> accumulator) {
            // 没来一条数据 pv+1，将user放入到HashSet中
            accumulator.f1.add(value.user);
            return Tuple2.of(accumulator.f0+1,accumulator.f1);
        }

        @Override
        public Double getResult(Tuple2<Long, HashSet> accumulator) {
            //触发窗口时，输出pv和uv的比值
            return (double)accumulator.f0/accumulator.f1.size();
        }

        @Override
        public Tuple2<Long, HashSet> merge(Tuple2<Long, HashSet> a, Tuple2<Long, HashSet> b) {
            return null;
        }
    }

}