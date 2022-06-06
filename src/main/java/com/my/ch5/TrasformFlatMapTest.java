package com.my.ch5;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TrasformFlatMapTest {

    public static void main(String[] args)  throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream= env.fromElements(new Event("Mary", "/home", 1000L), (new Event("Bob", "/cart", 2000L)),(new Event("Alice","/prod?id=100",3000L)));

        //1 自定义function

        SingleOutputStreamOperator<String> result1 = stream.flatMap(new MyFlatMap());

        //2.传入Lambda表达式

        stream.flatMap((Event value, Collector<String> out) ->
        {
            if (value.user.equals("Bob"))
            {
            out.collect(value.user);
            out.collect(value.url);
            out.collect(value.timestamp.toString());
            }
            else
                out.collect(value.user);

        }).returns(new TypeHint<String>() {}).print("2");

        //result2.print();
        env.execute();

    }
    
    public static class MyFlatMap implements FlatMapFunction<Event,String>{
        @Override
        public void flatMap(Event value, Collector<String> out) throws Exception {

            out.collect(value.user);
            out.collect(value.url);
            out.collect(value.timestamp.toString());

        }
    }
}
