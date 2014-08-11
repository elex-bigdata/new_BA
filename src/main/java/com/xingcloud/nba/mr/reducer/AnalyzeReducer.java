package com.xingcloud.nba.mr.reducer;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by wanghaixing on 14-8-1.
 */
public class AnalyzeReducer extends Reducer<Text, Text, Text, NullWritable> {

    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        context.write(key, NullWritable.get());
    }

    /*protected void cleanup(Context context) throws IOException ,InterruptedException {
        System.out.println(uidCounter.getValue());
    }*/
}
