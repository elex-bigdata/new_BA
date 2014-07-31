package com.xingcloud.nba.mr.mapper;

import com.xingcloud.nba.mr.model.JoinData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by wanghaixing on 14-7-29.
 */
public class ActiveMapper extends Mapper<LongWritable, Text, Text, JoinData> {
    private static Log LOG = LogFactory.getLog(ActiveMapper.class);
    private JoinData joinData = new JoinData();


    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final Counter missOrgidCounter = context.getCounter("miss orgid", "missCount");
        String pathName = ((FileSplit)context.getInputSplit()).getPath().toString();
        Text joinKey = new Text();
        Text flag = new Text();
        Text secondPart = new Text();
        if(pathName.endsWith("stream_*.log")) {
            String[] items = value.toString().split("\t");
            flag.set("0");
            joinKey.set(items[1]);
            secondPart.set(items[1]);
            joinData = new JoinData(joinKey, flag, secondPart);
            context.getCounter("stream","log").increment(1);
        } else if(pathName.endsWith("id_map.txt")) {
            String[] items = value.toString().split("\t");
            if(items.length == 2) {
                flag.set("1");
                joinKey.set(items[0]);
                secondPart.set(items[1]);
            } else {
                missOrgidCounter.increment(1L);
                flag.set("1");
                joinKey.set(items[0]);
                secondPart.set(new Text("***"));
            }
            joinData = new JoinData(joinKey, flag, secondPart);
        }else{
            context.getCounter("miss","path").increment(1);
        }
        context.write(joinKey, joinData);
    }


}

