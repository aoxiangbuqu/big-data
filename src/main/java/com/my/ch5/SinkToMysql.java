package com.my.ch5;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkToMysql {

    public static void main(String[] args) throws Exception{
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Event> stream= env.fromElements(new Event("Mary", "/home", 1000L),
                (new Event("Bob", "/cart", 2000L)),
                (new Event("Alice","/prod?id=100",3000L)),
                (new Event("Bob","/prod?id=10",3200L)),
                (new Event("Bob","/prod?id=20",3500L)),
                (new Event("Alice","/prod?id=12",31000L))
        );
        stream.addSink(JdbcSink.sink(
                "insert into clicks(user,url) values (?,?) ",
                ((statement,event) -> {
                 statement.setString(1,event.user);
                 statement.setString(2,event.url);

                } ),

                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/flink?useSSL=false")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("flink")
                        .withPassword("flink")
                        .build()
        )
        );

        env.execute();

    }
}
