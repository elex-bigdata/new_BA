package com.xingcloud.nba.mr.mapper;

import com.xingcloud.nba.mr.model.JoinData;
import com.xingcloud.nba.utils.Constant;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by wanghaixing on 14-8-2.
 */
public class ProjectMapper extends Mapper<LongWritable, Text, Text, JoinData> {
    private static Log LOG = LogFactory.getLog(ProjectMapper.class);
    private JoinData joinData = new JoinData();

    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        Counter missOrgidCounter = context.getCounter("miss orgid", "missCount");
        Text joinKey = new Text();
        Text flag = new Text();
        Text secondPart = new Text();

        if (key.get() == Constant.KEY_FOR_EVENT_LOG) {
            String[] items = value.toString().split("\t");
            if(items[2].startsWith("visit.")) {
                flag.set("0");
                joinKey.set(items[1]);
                secondPart.set(items[1]);
                joinData = new JoinData(joinKey, flag, secondPart);
                context.getCounter("stream","log").increment(1);
                context.write(joinKey, joinData);
            }

        } else if (key.get() == Constant.KEY_FOR_IDMAP) {
            String[] items = value.toString().split("\t");
            if(items.length == 2) {
                flag.set("1");
                joinKey.set(items[0]);
                secondPart.set(items[1]);
            } else {
//                missOrgidCounter.increment(1L);
                flag.set("1");
                joinKey.set(items[0]);
                secondPart.set(new Text("***"));
            }
            joinData = new JoinData(joinKey, flag, secondPart);
            context.write(joinKey, joinData);
        }

    }
}
