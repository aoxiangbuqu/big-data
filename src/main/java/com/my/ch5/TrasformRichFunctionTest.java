package com.my.ch5;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TrasformRichFunctionTest {

    public static void main(String[] args)  throws  Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Event> stream= env.fromElements(new Event("Mary", "/home", 1000L), (new Event("Bob", "/cart", 2000L)),(new Event("Alice","/prod?id=100",3000L)));

        stream.map(new MyRichMapper() {
        }).print();

        env.execute();

    }

    //实现一个自定义的富函数类

    public static class MyRichMapper extends  RichMapFunction <Event,Integer>{

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open 生命周期"+getRuntimeContext().getIndexOfThisSubtask()+"任务启动");
        }

        @Override
        public Integer map(Event value) throws Exception {
            return value.url.length();
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("open 生命周期"+getRuntimeContext().getIndexOfThisSubtask()+"任务结束");

        }
    }

}
