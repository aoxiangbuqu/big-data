package com.my.ch11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;


public class CommonApiTest {

    public static void main(String[] args) throws Exception{
        //原始
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment envTable = StreamTableEnvironment.create(env);

        //1、定义环境配置来创建表执行环境
        EnvironmentSettings setting = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();

        TableEnvironment tableEnv = TableEnvironment.create(setting);


        //2、创建表,连接器表

        String DDl = "create table  clicktable (" +
                "user_name String,url String,ts bigint)" +
                "with (" +
                "'connector'='filesystem'," +
                "'path' = 'input/click'," +
                "'format' = 'csv'" +
                ")";

        tableEnv.executeSql(DDl);


        //一、调用tableAPi查询表的转化注册的表与table 对象转化的关系
        Table clicktable = tableEnv.from("clicktable");
        Table resultTable = clicktable.where($("user_name").isEqual("Bob"))
                .select($("user_name"), $("url"));
        //再可以转化为表
        tableEnv.createTemporaryView("clickt", resultTable);

        //执行sql查询转化,这块查询的必须是表，要么是tableapi转化的表 要么是注册的表
        Table tableResult = tableEnv.sqlQuery("select user_name,url from clicktable");
         //执行聚合计算的查询
        Table aggResult = tableEnv.sqlQuery("select user_name,count(1) as cn  from clicktable group by user_name");

        //3、创建一张用户输出的表
        String outDDl = "create table  outTable (" +
                "user_name String,url String)" +
                "with (" +
                "'connector'='filesystem'," +
                "'path' = 'output'," +
                "'format' = 'csv'" +
                ")";

        tableEnv.executeSql(outDDl);

        //、创建一张用户控制台打印的表

        String printDDl = "create table  printTable (" +
                "user_name String,cn bigint)" +
                "with (" +
                "'connector'='print'"+
                ")";

        tableEnv.executeSql(printDDl);

        //输出表
        aggResult.executeInsert("printTable");
    }
}
