package com.my.ch11;

import com.my.ch5.ClickSource;
import com.my.ch5.Event;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class SimpleTableExample {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> eventStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));

        //2、创建一个表执行环境

        StreamTableEnvironment tableEnv= StreamTableEnvironment.create(env);

        //3.stream转化为表
        Table eventTable = tableEnv.fromDataStream(eventStream);

        //4、直接写sql进行转化

        Table resultTable = tableEnv.sqlQuery("select user,url from " + eventTable);
        //5、tableapi
        Table resultTable2 = eventTable.select($("user"), $("url"))
                .where($("user").isEqual("Bob"));

        //7、聚合转化
        tableEnv.createTemporaryView("clicktable",eventTable);
        Table aggResult = tableEnv.sqlQuery("select user,count(1) as cn  from clicktable group by user");

        //表转化为流输出
        tableEnv.toChangelogStream(aggResult).print("result1");


        //tableEnv.toDataStream(resultTable).print("result1");
        //tableEnv.toDataStream(resultTable2).print("result2");

        env.execute();


    }
}
