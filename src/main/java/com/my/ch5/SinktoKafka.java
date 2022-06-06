package com.my.ch5;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Locale;
import java.util.Properties;

public class SinktoKafka {

    public static void main(String[] args)  throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //1.从kafka读取数据
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        DataStreamSource<String> kafkastream = env.addSource(new FlinkKafkaConsumer<String>("test",new SimpleStringSchema(),properties));

        //用filnk进行转化处理

        SingleOutputStreamOperator<String> result = kafkastream.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                String[] fileds = value.split(",");
                return new Event(fileds[0].trim(), fileds[1].trim(), Long.valueOf(fileds[2])).toString();
            }
        });
       //写入到kafka
        result.addSink(new FlinkKafkaProducer<String>("localhost:9092","event",new SimpleStringSchema()));

       env.execute();
    }
}
