package com.xingcloud.nba.mr.mapper;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * Created by wanghaixing on 14-8-1.
 */
public class AnalyzeMapper extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException {
        String pathName = ((FileSplit)context.getInputSplit()).getPath().toString();
        if(pathName.contains("part-r")) {
            context.write(value, new Text(""));
            context.getCounter("inputFiles", "files").increment(1L);
        }
    }
}
