package com.my.ch07;

import com.my.ch5.ClickSource;
import com.my.ch5.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class ProcessFuntionTest {
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

//        //stream.process(new ProcessFunction<Event, String>() {
//          //  @Override
//            //public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
//                if (value.user.equals("Mary")){
//                    out.collect(value.user+"clicks"+value.url);
//                } else if (value.user.equals("Bob")){
//                    out.collect(value.user);
//                    out.collect(value.timestamp.toString());
//                }
//
//                System.out.println("timestamp:"+ctx.timestamp());
//                System.out.println("watermark:"+ctx.timerService().currentWatermark());
//            }
//
//        }).print();

        stream.process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {

                    out.collect("Mary?????????url???"+value.url);
                    out.collect("Mary?????????time???"+value.timestamp);
                    out.collect("???????????????"+ctx.timestamp());
                    out.collect("??????????????????"+ctx.timerService().currentWatermark());

            }
        }).print();


        env.execute();
    }
}
