package com.xingcloud.nba.business;

import com.xingcloud.maincache.MapXCache;
import com.xingcloud.maincache.XCacheException;
import com.xingcloud.maincache.XCacheOperator;
import com.xingcloud.maincache.redis.RedisXCacheOperator;
import com.xingcloud.nba.utils.DateManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * Created by Administrator on 14-8-4.
 */
/**
 * COMMON,internet-1,2014-08-03,2014-08-03,visit.*,TOTAL_USER,VF-ALL-0-0,PERIOD
 * COMMON,internet-1,2014-07-27,2014-08-03,visit.*,TOTAL_USER,VF-ALL-0-0,PERIOD
 * COMMON,internet-1,2014-07-04,2014-08-03,visit.*,TOTAL_USER,VF-ALL-0-0,PERIOD
 */
public class StoreResult {
    private static Log LOG = LogFactory.getLog(StoreResult.class);
    private String[] dates = new String[3];
    private String dayDate;
    private String weekDate;
    private String monthDate;
    private List<String> keyList = new ArrayList<String>();

    public StoreResult() {
        setup();
    }

    public void setup() {
        dates[0] = DateManager.getDaysBefore(1, 0);     //2014-08-03
        dates[1] = DateManager.getDaysBefore(8, 0);    //2014-07-27
        dates[2] = DateManager.getDaysBefore(31, 0);  //2014-07-04

        String key = "";
        for(int i = 0; i < 3; i++) {
            key = "COMMON,internet-1," + dates[i] + "," + dates[0] + ",visit.*,TOTAL_USER,VF-ALL-0-0,PERIOD";
            keyList.add(key);
        }
    }


    public void store(long[] counts) {
        Map<String, Number[]> result = null;
        MapXCache xCache = null;
        XCacheOperator xCacheOperator = RedisXCacheOperator.getInstance();
        try {
            for(int i = 0; i < 3; i++) {
                result = new HashMap<String, Number[]>();
                result.put(keyList.get(i), new Number[]{0, 0, counts[i], 1.0});
                xCache = MapXCache.buildMapXCache(keyList.get(i), result);
                xCacheOperator.putMapCache(xCache);
            }

        } catch (XCacheException e) {
            e.printStackTrace();
        }
    }




}
