package com.my.ch11;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

public class UdfTest_Aggre {
    public static void main(String[] args) throws Exception {
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

        tableEnv.createTemporaryFunction("WeightedAvg", WeightedAvg.class);

        //3、调用UDF进行查询转化

        Table result = tableEnv.sqlQuery("select user_name,WeightedAvg(ts,1) as w_avg from " +
                " clicktable group by user_name");

        //4、输出打印结果

        tableEnv.toChangelogStream(result).print();
        env.execute();
    }

    //单独定义累加器类
    public static  class WeightedAvgAccumulator{
        public long sum=0;
        public int count =0;

    }
    //实现自定义的的聚合函数，计算加权平均值

    public static class WeightedAvg extends AggregateFunction<Long,WeightedAvgAccumulator>{
        @Override
        public Long getValue(WeightedAvgAccumulator accumulator) {
            if (accumulator.count ==0)
                return null;
            else
                return accumulator.sum /accumulator.count;
        }

        @Override
        public WeightedAvgAccumulator createAccumulator() {
            return new WeightedAvgAccumulator();
        }

        //累加器计算的方法

        public  void accumulate(WeightedAvgAccumulator accumulator,Long iValue,Integer iWeight){
            accumulator.sum = iValue*iWeight;
            accumulator.count += iWeight;
        }
    }


}
