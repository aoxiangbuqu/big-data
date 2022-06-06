package com.my.ch5;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

public class ClickSource implements SourceFunction <Event> {
    private  Boolean running = true;
    @Override
    public void run(SourceContext ctx) throws Exception {

        Random random = new Random();
        //定义字段选取的数据集
        String[] users = {"Mary","Bob","Allice"};
        String[] urls = {"/home","/cart","/prod?id=100"};

        while (running){

            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Long timestamp = Calendar.getInstance().getTimeInMillis();

            ctx.collect(new Event(user,url,timestamp));
            Thread.sleep(1000L);


        }
    }

    @Override
    public void cancel() {
        running =false;


    }
}
