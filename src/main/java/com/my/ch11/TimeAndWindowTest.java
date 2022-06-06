package com.my.ch11;

import com.my.ch5.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import com.my.ch5.Event;
import java.time.Duration;
import static org.apache.flink.table.api.Expressions.$;

public class TimeAndWindowTest {
    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1、创建表的DDL中直接定义时间属性

        String DDl = "create table  clicktable (" +
                "user_name String,url String,ts bigint," +
                "et AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000))," +
                "WATERMARK FOR et as et - INTERVAL '1' SECOND)" +
                "with (" +
                "'connector'='filesystem'," +
                "'path' = 'input/click'," +
                "'format' = 'csv'" +
                ")";
        tableEnv.executeSql(DDl);
      //2、在流转化为表时间的时候 定义时间属性
        SingleOutputStreamOperator<Event> clickStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.timestamp;
                            }
                        }));
        Table clickTable = tableEnv.fromDataStream(clickStream, $("user"), $("url"), $("timestamp").as("ts"),
                $("et").rowtime());

        //1、分组聚合
        Table aggTable = tableEnv.sqlQuery("select user_name,count(1) from clicktable group by user_name");
       // 2、分组窗口聚合，历史版本有，1.13版本基本不用

        Table aggGroupResult = tableEnv.sqlQuery("select " +
                "  user_name,count(1) as cnt," +
                "  TUMBLE_End(et,INTERVAL '10' SECOND) as endT" +
                " from clicktable group by user_name," +
                "TUMBLE(et,INTERVAL '10' SECOND) "
        );

        // 3、新版本窗口聚合

        Table aggWindow = tableEnv.sqlQuery("select user_name,count(1) as cnt," +
                " window_end as endT " +
                "from TABLE(" +
                " TUMBLE(TABLE clicktable,DESCRIPTOR(et),INTERVAL '10' SECOND) " +
                ")" +
                "group by user_name,window_end,window_start "
        );

        //3、1累计窗口

        Table accuWindow = tableEnv.sqlQuery("select user_name,count(1) as cnt," +
                " window_end as endT " +
                "from TABLE(" +
                " CUMULATE(TABLE clicktable,DESCRIPTOR(et),INTERVAL '10' SECOND,INTERVAL '30' SECOND) " +
                ")" +
                "group by user_name,window_end,window_start "
        );

        //4、开窗聚合

        Table overWindow = tableEnv.sqlQuery("select user_name," +
                "avg(ts) over (partition by user_name order by et" +
                "  rows between 3 preceding and current row) as avg_ts" +
                "   from clicktable ");

        clickTable.printSchema();

        tableEnv.toChangelogStream(overWindow).print();

        env.execute();


    }
}
