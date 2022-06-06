package com.my.ch11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

public class UdfTest_ScalarFunction {
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

        //2、注册定义函数

        tableEnv.createTemporaryFunction("MyHash",MyHashFunction.class);

        //3、调用UDF进行查询转化

        Table result = tableEnv.sqlQuery("select user_name,MyHash(user_name) from clicktable");

        //4、输出打印结果

        tableEnv.toDataStream(result).print();
        env.execute();
    }

    //自定义实现ScalarFunction

    public static class MyHashFunction extends ScalarFunction{

        public int eval(String str){
            return str.hashCode();
        }
    }
}
