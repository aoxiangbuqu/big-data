package com.my.ch5;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Test {
    public static void main(String[] args)  throws Exception{

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "/home", 1000L),
            (new Event("Bob", "/cart", 2000L)),
            (new Event("Alice", "/prod?id=100", 3000L)),
            (new Event("Bob", "/prod?id=10", 3200L)),
            (new Event("Bob", "/prod?id=20", 3500L)),
            (new Event("Alice", "/prod?id=12", 31000L))
    );
        SingleOutputStreamOperator<Tuple2<String, Integer>> result1 = stream.map(new MapFunction<Event, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(Event value) throws Exception {
                return Tuple2.of(value.user, 1);
            }
        }).keyBy(data -> data.f0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                });
        result1.print("汇总：");

////        result1.keyBy(data -> "key")
//                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
//                    @Override
//                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
//                        return value1.f1 > value2.f1 ? value1 : value2;
//                    }
//                }).print();




        env.execute();
    }
}
