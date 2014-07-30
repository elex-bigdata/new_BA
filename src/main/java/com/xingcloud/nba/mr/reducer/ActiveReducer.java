package com.xingcloud.nba.mr.reducer;

import com.xingcloud.nba.mr.model.JoinData;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;


/**
 * Created by wanghaixing on 14-7-29.
 */
public class ActiveReducer extends Reducer<Text, JoinData, Text, Text> {
    private static Log LOG = LogFactory.getLog(ActiveReducer.class);
    private ArrayList<Text> firstTable = new ArrayList<Text>();
    private ArrayList<Text> secondTable = new ArrayList<Text>();
    private Text secondPart = null;
    private Text output = new Text();

    protected void reduce(Text key, Iterable<JoinData> values, Context context) throws IOException, InterruptedException {
        firstTable.clear();
        secondTable.clear();

        for(JoinData jd : values) {
            secondPart = jd.getSecondPart();
            if("0".equals(jd.getFlag().toString().trim())) {
                firstTable.add(secondPart);
            } else if("1".equals(jd.getFlag().toString().trim())) {
                secondTable.add(secondPart);
            }
        }

        for(Text orgid : secondTable) {
            output.set(orgid);
            context.write(key, output);
        }
    }
}
