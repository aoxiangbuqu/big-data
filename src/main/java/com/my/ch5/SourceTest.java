package com.my.ch5;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.ArrayList;
import java.util.Properties;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

public class SourceTest {

    public static void main(String[] args)  throws  Exception{
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1.从文件中读取数据
        DataStreamSource<String> stream1 = env.readTextFile("input/click");
        //2.从集合中读取
        ArrayList<Integer> nums = new ArrayList<>();
        nums.add(2);
        nums.add(5);
        DataStreamSource<Integer> numstream = env.fromCollection(nums);

        ArrayList<Event> events = new ArrayList<>();
        events.add(new Event("Mary","/home",1000L));
        events.add(new Event("Bob","/cart",2000L));
        DataStreamSource<Event> stream2 = env.fromCollection(events);
        //3.从元素中获取
        DataStreamSource<Event> stream3 = env.fromElements(new Event("Mary", "/home", 1000L), (new Event("Bob", "/cart", 2000L)));

        //4.从kafka读取
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092");
        DataStreamSource<String> kafkastream = env.addSource(new FlinkKafkaConsumer<String>("test",new SimpleStringSchema(),properties));
        //stream1.print();
        //numstream.print();
        //stream2.print();
        kafkastream.print();
        env.execute();






    }
}
