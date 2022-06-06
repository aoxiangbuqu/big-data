package com.my.ch11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TopNExample {
    public static void main(String[] args)  throws  Exception{
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

        //普通TopN，选取当前所有用户中浏览量最大的2个

        Table topNResult = tableEnv.sqlQuery("select user_name,cnt,row_num from ( " +
                " select *,row_number() over ( " +
                "order by cnt desc) as row_num from (" +
                "select user_name,count(1) as cnt from clicktable " +
                "group by user_name )" +
                ") where row_num <=2");

        //窗口topn

        String subQuery ="select user_name,count(1) as cnt," +
                " window_end,window_start  " +
                "from TABLE(" +
                " TUMBLE(TABLE clicktable,DESCRIPTOR(et),INTERVAL '10' SECOND) " +
                ")" +
                "group by user_name,window_end,window_start ";
        Table topNWindowResult = tableEnv.sqlQuery("select user_name,window_end,cnt,row_num from ( " +
                " select *,row_number() over ( " +
                "partition by window_start,window_end order by cnt desc) as row_num from ("+ subQuery+
                ") " +
                ") where row_num <=2");

        tableEnv.toChangelogStream(topNWindowResult).print();

        env.execute();

    }
}
