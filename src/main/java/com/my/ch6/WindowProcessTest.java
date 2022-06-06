package com.my.ch6;

import com.my.ch5.ClickSource;
import com.my.ch5.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

public class WindowProcessTest {
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
        //使用ProcessWindowFunction 计算UV
        stream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .process(new UvCountByWindow())
                .print();

        env.execute();
    }
    //实现自定义的processWindowfunction 输出一条信息
    public  static class UvCountByWindow extends ProcessWindowFunction<Event,String,Boolean, TimeWindow>{
        @Override
        public void process(Boolean aBoolean, Context context, Iterable<Event> elements, Collector<String> out) throws Exception {

            //用hashSet保存user
            HashSet<String> userSet = new HashSet<>();
            //从element中遍历数据放到userSet中
            Integer cnt = 0;
            for (Event event:elements){
                userSet.add(event.user);
                cnt = cnt+1;
            }


            Integer uv = userSet.size();
            //结合窗口信息

            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            out.collect("窗口："+new Timestamp(start)+"~"+new Timestamp(end)
                    +"  UV值："+uv+" pv:"+cnt);


        }
    }
}