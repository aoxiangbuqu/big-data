package com.my.wc;
//dataStrem api

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class BoundesStreamWordCount {

    public static void main(String[] args) throws  Exception {
        //1、创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2、读取文件
        DataStreamSource<String> lineDataSource = env.readTextFile("input/words.txt");

        //3、计算
        SingleOutputStreamOperator<Tuple2<String, Long>> wordOneTupe = lineDataSource.flatMap((String line, Collector<Tuple2<String, Long>> out) -> {
                    String[] words = line.split(" ");
                    //将每个单词转化成二元组输出
                    for (String word : words) {
                        out.collect(Tuple2.of(word, 1L));
                    }
                }

        ).     //return 类型提示 不然不知道tuple中的具体数据类型
                returns(Types.TUPLE(Types.STRING, Types.LONG));
     //4、计算
        KeyedStream<Tuple2<String, Long>, String> wordAndOneKeyedStream = wordOneTupe.keyBy(data -> data.f0);
   //5、求和
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = wordAndOneKeyedStream.sum(1);
  // 6、打印
        sum.print();

    //7 执行
        env.execute();
    }
}
