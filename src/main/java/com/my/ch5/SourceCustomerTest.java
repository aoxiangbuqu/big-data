package com.my.ch5;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.util.Random;

public class SourceCustomerTest {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);

        DataStreamSource<Integer> customerStream = env.addSource(new ParalleCustomerSource()).setParallelism(2);

        customerStream.print();

        env.execute();

    }

    public static class ParalleCustomerSource implements ParallelSourceFunction<Integer>{
        private  Boolean running = true;
        private Random random = new Random();
        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running)
                ctx.collect(random.nextInt());

        }

        @Override
        public void cancel() {
            running = false;

        }
    }

}


