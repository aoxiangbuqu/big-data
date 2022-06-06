package com.my.ch5;

import akka.remote.WireFormats;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.DefaultBucketFactoryImpl;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import javax.xml.stream.StreamFilter;
import java.util.concurrent.TimeUnit;

public class SinkFileTest {

    public static void main(String[] args)  throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "/home", 1000L),
                (new Event("Bob", "/cart", 2000L)),
                (new Event("Alice", "/prod?id=100", 3000L)),
                (new Event("Bob", "/prod?id=10", 3200L)),
                (new Event("Bob", "/prod?id=20", 3500L)),
                (new Event("Alice", "/prod?id=12", 31000L))
        );

        StreamingFileSink<String> streamFileSink = StreamingFileSink.<String>forRowFormat(new Path("./output"),
                new SimpleStringEncoder<>("UTF8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withMaxPartSize(1024 * 1024 * 1024)
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(5))
                                .build()
                )
                .build();

       stream.map(data -> data.toString()).addSink(streamFileSink);

       env.execute();
    }
}
