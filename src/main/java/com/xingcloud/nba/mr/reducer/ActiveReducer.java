package com.xingcloud.nba.mr.reducer;

import com.xingcloud.nba.mr.model.JoinData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;


/**
 * Created by wanghaixing on 14-7-29.
 */
public class ActiveReducer extends Reducer<Text, JoinData, Text, NullWritable> {
    private static Log LOG = LogFactory.getLog(ActiveReducer.class);


    protected void reduce(Text key, Iterable<JoinData> values, Context context) throws IOException, InterruptedException {
//        firstTable.clear();
//        secondTable.clear();

        Set<String> firstTable = new HashSet<String>();
        Set<String> secondTable = new HashSet<String>();
        Text secondPart = null;
        Text output = new Text();
        String flag;

        for(JoinData jd : values) {
            flag = jd.getFlag().toString().trim();
            secondPart = jd.getSecondPart();
            if("0".equals(flag)) {
                firstTable.add(secondPart.toString());
            } else if("1".equals(flag)) {
                secondTable.add(secondPart.toString());
            }
        }

        LOG.info("tb_dim_city:"+firstTable.toString());
        LOG.info("tb_user_profiles:"+secondTable.toString());


        if(firstTable.size() == 0) {
            return;
        }

        for(String uid : firstTable) {
            for(String orgid : secondTable) {
                System.out.println(orgid);
                output.set(uid + "\t" + orgid);
                context.write(output, NullWritable.get());
            }
        }

    }
}
