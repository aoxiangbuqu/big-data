package com.my.ch09;

import akka.stream.impl.ReducerState;
import akka.stream.impl.fusing.Reduce;
import com.my.ch5.ClickSource;
import com.my.ch5.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class StateTest {
    public static void main(String[] args) throws Exception{
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
        stream.keyBy(data -> data.user)
                .flatMap(new MyFlatMap())
                .print();

        env.execute();

    }

    //实现自定义的FlatMapFunction 用作keyed state测试
    public static class MyFlatMap extends RichFlatMapFunction<Event,String>{
        ValueState<Event>  myValueState;
        ListState<Event> myListState;
        MapState<String,Long> myMapState;
        ReducingState<Event> myReducingState;
        AggregatingState<Event,String> myAggregatingState;



        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Event> eventValueStateDescriptor = new ValueStateDescriptor<>("my-state", Event.class);
            myValueState = getRuntimeContext().getState(eventValueStateDescriptor);


            myListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<Event>("my-list-state",Event.class)
            );
            myMapState = getRuntimeContext().getMapState(
                    new MapStateDescriptor<String, Long>("my-map-state",String.class,Long.class)
            );
            myReducingState = getRuntimeContext().getReducingState(
                    new ReducingStateDescriptor<Event>("my-reduce-state",
                            new ReduceFunction<Event>() {
                                @Override
                                public Event reduce(Event value1, Event value2) throws Exception {
                                    return new Event(value1.user,value1.url,value2.timestamp);
                                }
                            },
                            Event.class)
            );

            myAggregatingState = getRuntimeContext().getAggregatingState(
                    new AggregatingStateDescriptor<Event, Long, String>("my-agg-state",
                            new AggregateFunction<Event, Long, String>() {
                                @Override
                                public Long createAccumulator() {
                                    return 0L;
                                }

                                @Override
                                public Long add(Event value, Long accumulator) {
                                    return accumulator+1;
                                }

                                @Override
                                public String getResult(Long accumulator) {
                                    return "count:"+accumulator;
                                }

                                @Override
                                public Long merge(Long a, Long b) {
                                    return null;
                                }
                            },
                            Long.class)
            );


            //配置状态的TTL
            StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.hours(1))
                    .setUpdateType(StateTtlConfig.UpdateType.OnReadAndWrite)
                    .build();

            eventValueStateDescriptor.enableTimeToLive(ttlConfig);
        }

        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {

            //访问和更新状态
           // System.out.println("my1"+myValueState.value());
            //每次来数据都更新状态
            myValueState.update(value);
            //System.out.println("my2"+myValueState.value());

            myListState.add(value);

            System.out.println("my-list:"+myListState.get());

            myMapState.put(value.user, myMapState.get(value.user)==null? 1 :myMapState.get(value.user)+1);
            System.out.println("my-map:"+value.user+" "+myMapState.get(value.user));

            myAggregatingState.add(value);
            System.out.println("my-agg:"+myAggregatingState.get());

            myReducingState.add(value);
            System.out.println("my-reduce:"+myReducingState.get());




        }
    }
}
