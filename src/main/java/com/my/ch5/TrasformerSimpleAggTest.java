package com.my.ch5;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TrasformerSimpleAggTest {

    public static void main(String[] args) throws  Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream= env.fromElements(new Event("Mary", "/home", 1000L),
                (new Event("Bob", "/cart", 2000L)),
                (new Event("Alice","/prod?id=100",3000L)),
                (new Event("Bob","/prod?id=10",3200L)),
                (new Event("Bob","/prod?id=20",3500L)),
                (new Event("Alice","/prod?id=12",31000L))
                );
        //按键分组之后进行聚合，提取当前用户的最近一次访问数据

        //max和maxby的区别：都是取日期最大的 但是maxby是取最大那条的url，max是取第一条url
        stream.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event value) throws Exception {
                return value.user;
            }
        }).max("timestamp").print("max: ");

        stream.keyBy( data -> data.user).maxBy("timestamp").print("maxby:");


        env.execute();
    }
}
