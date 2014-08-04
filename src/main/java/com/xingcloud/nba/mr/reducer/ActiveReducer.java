package com.xingcloud.nba.mr.reducer;

import com.xingcloud.nba.mr.model.JoinData;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by wanghaixing on 14-7-29.
 */
public class ActiveReducer extends Reducer<Text, Text, Text, NullWritable> {

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Counter counter = context.getCounter("ActiveCounter", "uidCounts");
        counter.increment(1L);
        context.write(key, NullWritable.get());
    }
}

