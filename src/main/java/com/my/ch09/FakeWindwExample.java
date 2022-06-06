package com.my.ch09;

import com.my.ch5.ClickSource;
import com.my.ch5.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

public class FakeWindwExample {
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
        stream.keyBy(data -> data.url)
                .process(new FakeWindwResult(10000L))
                .print();
        stream.print("input");

        env.execute();
    }

    public static  class FakeWindwResult extends KeyedProcessFunction<String,Event,String>{

        private Long windowSize;


        public FakeWindwResult(Long windowSize) {
            this.windowSize = windowSize;
        }

        //定义一个mapState，用来保存每个窗口的统计count值
        MapState<Long,Long> windowUrlMapState;

        @Override
        public void open(Configuration parameters) throws Exception {
            windowUrlMapState=getRuntimeContext().getMapState(new MapStateDescriptor<Long, Long>("window-count",Long.class,Long.class));

        }

        @Override
        public void processElement(Event value, Context ctx, Collector<String> out) throws Exception {
            //每来一条数据，根据时间戳判断属于哪一个窗口（窗口分配器）
            Long windowStart = ctx.timestamp()/windowSize * windowSize;
            Long windowEnd = windowStart+windowSize;

            //注册end-1的定时器

            ctx.timerService().registerEventTimeTimer(windowEnd-1);

            //更新状态，进行增量聚合

            if (windowUrlMapState.contains(windowStart)){
                Long count=windowUrlMapState.get(windowStart);
                windowUrlMapState.put(windowStart,count+1);
            } else
            {
                windowUrlMapState.put(windowStart,1L);
            }

        }

        //定时器触发时输出计算结果

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            Long windowEnd = timestamp+1;
            Long windowStart = windowEnd-windowSize;
            Long count = windowUrlMapState.get(windowStart);

            out.collect("窗口："+new Timestamp(windowStart)+"~"+new Timestamp(windowEnd) +
                    "url"+ctx.getCurrentKey()+":"+count);
            //模拟窗口的关闭，清除map中对应的key-value

            windowUrlMapState.remove(windowStart);

        }
    }
}
