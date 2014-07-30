package com.xingcloud.nba.mr.mapper;

import com.xingcloud.nba.mr.model.JoinData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by wanghaixing on 14-7-29.
 */
public class ActiveMapper extends Mapper<LongWritable, Text, Text, JoinData> {
    private static Log LOG = LogFactory.getLog(ActiveMapper.class);
    private JoinData joinData = null;
    private Text joinKey = new Text();
    private Text flag = new Text();
    private Text secondPart = new Text();

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String pathName = ((FileSplit)context.getInputSplit()).getPath().toString();
        if(pathName.endsWith("stream_*.log")) {
            String[] items = value.toString().split("\t");
            flag.set("0");
            joinKey.set(items[1]);
            secondPart.set(items[1]);
            joinData = new JoinData(joinKey, flag, secondPart);
        } else if(pathName.endsWith("id_map.txt")) {
            String[] items = value.toString().split("\t");
            flag.set("1");
            joinKey.set(items[0]);
            secondPart.set(items[1]);
            joinData = new JoinData(joinKey, flag, secondPart);
        }
        context.write(joinKey, joinData);
    }


}

