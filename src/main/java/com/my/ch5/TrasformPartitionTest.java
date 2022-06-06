package com.my.ch5;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

public class TrasformPartitionTest {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.fromElements(new Event("Mary", "/home", 1000L),
                (new Event("Bob", "/cart", 2000L)),
                (new Event("Alice", "/prod?id=100", 3000L)),
                (new Event("Bob", "/prod?id=10", 3200L)),
                (new Event("Bob", "/prod?id=20", 3500L)),
                (new Event("Alice", "/prod?id=12", 31000L))
        );
      // 1、随机分区，比较均匀，洗牌
        //stream.shuffle().print().setParallelism(4);
        //2、轮询分区，发牌
        //前后两个任务并行度不同，flink底层是默认是rebalance来进行重分区
        //stream.rebalance().print().setParallelism(2);
        //3、rescale 重缩放分区，先分组，再重分区
       /** env.addSource(new RichParallelSourceFunction<Integer>() {
            @Override
            public void run(SourceContext<Integer> ctx) throws Exception {
                for (int i = 1;i <=8;i++) {
                    //将奇偶数分别发送0号和1号子任务
                    if (i%2 == getRuntimeContext().getIndexOfThisSubtask())
                        ctx.collect(i);


                }
            }

            @Override
            public void cancel() {

            }
        }).setParallelism(2).rescale().print().setParallelism(4);
        **/
        //4.广播,每一个分区有全部数据
        //stream.broadcast().print().setParallelism(4);

        //5、全局分区，全部数据分到1个分区
        //stream.global().print().setParallelism(4);
        //6、自定义重新分区
        env.fromElements(1,2,3,4,5,6,7,8)
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key%2;
                    }
                }, new KeySelector<Integer, Integer>() {
                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                }).print().setParallelism(4);
        env.execute();
    }
}
