package com.xingcloud.nba.mr.reducer;

import com.xingcloud.nba.mr.model.JoinData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by Administrator on 14-8-2.
 */
public class ProjectReducer extends Reducer<Text, JoinData, Text, NullWritable> {
    private static Log LOG = LogFactory.getLog(ProjectReducer.class);
    private Set<String> firstTable = new HashSet<String>();
    private Set<String> secondTable = new HashSet<String>();
    private Text secondPart = null;
    private Text output = new Text();
    private String flag;

    protected void reduce(Text key, Iterable<JoinData> values, Context context) throws IOException, InterruptedException {
        Counter table1Counter = context.getCounter("firsttable", "Count");
        Counter table2Counter = context.getCounter("secondtable", "Count");
        firstTable.clear();
        secondTable.clear();

        for(JoinData jd : values) {
            flag = jd.getFlag().toString().trim();
            secondPart = jd.getSecondPart();
            if("0".equals(flag)) {
                firstTable.add(secondPart.toString());
                table1Counter.increment(1L);
            } else if("1".equals(flag)) {
                secondTable.add(secondPart.toString());
                table2Counter.increment(1L);
            }
        }


        LOG.info("tb_dim_city:"+firstTable.toString());
        LOG.info("tb_user_profiles:"+secondTable.toString());


        if(firstTable.size() > 0) {
            for(String uid : firstTable) {
                for(String orgid : secondTable) {
                    output.set(orgid);
                    context.write(output, NullWritable.get());
                }
            }
        }

    }
}
