package com.my.ch07;

import com.my.ch5.ClickSource;
import com.my.ch5.Event;
import com.my.ch6.UrlCountView;
import com.my.ch6.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.awt.*;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

public class TopNExample {
    public static void main(String[] args)  throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

    //1、按照url分组，统计窗口内每个url的访问量

        SingleOutputStreamOperator<UrlViewCount> urlCountStream = stream.keyBy(data -> data.url)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlCountView.UrlViewCountAgg(), new UrlCountView.UrlViewCountResult());

        urlCountStream.print("url count");
        //对于同一窗口统计出现的访问量，进行收集和排序
        urlCountStream.keyBy(data ->data.windowEnd)
                .process(new TopNProcessResult(1))
                .print();

        env.execute();

    }
    //实现自定义的keyedProcessFunction
  public static class TopNProcessResult extends KeyedProcessFunction<Long,UrlViewCount,String>{
        //定义一个属性
        private Integer n ;

        private ListState<UrlViewCount> urlCountViewListState;

        public TopNProcessResult(Integer n) {
            this.n = n;
        }

        //在环境中获取状态

        @Override
        public void open(Configuration parameters) throws Exception {
            urlCountViewListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<UrlViewCount>("url-count-list", Types.POJO(UrlViewCount.class))
            );
        }

        @Override
        public void processElement(UrlViewCount value, Context ctx, Collector<String> out) throws Exception {

            //将数据保存在状态中
            urlCountViewListState.add(value);
            //注册windowEnd+1ms的定时器
            ctx.timerService().registerEventTimeTimer(ctx.getCurrentKey()+1);

        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            ArrayList<UrlViewCount> result = new ArrayList<>();
            for (UrlViewCount urlViewCount:urlCountViewListState.get()){
                    result.add(urlViewCount);
            }
            result.sort(new Comparator<UrlViewCount>() {
                @Override
                public int compare(UrlViewCount o1, UrlViewCount o2) {
                    return o2.count.intValue() -o1.count.intValue()  ;
                }
            });

            //包装信息打印输出
            StringBuilder result1 = new StringBuilder();
            result1.append("-------------------------\n");
            result1.append("窗口结束时间："+new Timestamp(ctx.getCurrentKey())+ "\n");
            //取list前两个

            for (int i = 0;i<n;i++){

                UrlViewCount cuur = result.get(i);
                String info = "no"+(i+1)+" " +"url:"+cuur.url + " "
                        +"当前访问："+cuur.count;
                result1.append(info);
            }

            result1.append("-------------\n");

            out.collect(result1.toString());

        }
    }
}
