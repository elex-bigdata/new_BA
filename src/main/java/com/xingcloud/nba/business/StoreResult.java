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
 * COMMON,internet-1,2014-08-03,2014-08-03,visit.*,TOTAL_USER,VF-ALL-0-0,PERIOD 日活跃
 * COMMON,internet-1,2014-07-27,2014-08-03,visit.*,TOTAL_USER,VF-ALL-0-0,PERIOD 周活跃
 * COMMON,internet-1,2014-07-04,2014-08-03,visit.*,TOTAL_USER,VF-ALL-0-0,PERIOD 月活跃
 *
 * COMMON,internet-1,2014-08-02,2014-08-07,visit.*,{"register_time":{"$gte":"2014-08-01","$lte":"2014-08-01"}},VF-ALL-0-0,PERIOD    一周留存
 *
 * COMMON,internet-1,2014-08-04,2014-08-04,visit.*,{"register_time":{"$gte":"2014-08-04","$lte":"2014-08-04"}},VF-ALL-0-0,PERIOD    每日新增
 */
public class StoreResult {
    private static Log LOG = LogFactory.getLog(StoreResult.class);
//    private String[] dates = new String[3];
    private List<String> keyList = new ArrayList<String>();
    private String specialTask;

    public StoreResult(String specialTask) {
        this.specialTask = specialTask;
//        setup();
    }

    public void setup() {
        /*dates[0] = DateManager.getDaysBefore(1, 0);     //2014-08-03
        dates[1] = DateManager.getDaysBefore(8, 0);     //2014-07-27
        dates[2] = DateManager.getDaysBefore(31, 0);    //2014-07-04

        String key = "";
        for(int i = 0; i < 3; i++) {
            key = "COMMON," + specialTask + "," + dates[i] + "," + dates[0] + ",visit.*,TOTAL_USER,VF-ALL-0-0,PERIOD";
            keyList.add(key);
        }*/
    }


    /**
     * 保存日、周、月活跃
     * @param counts
     */
    public void storeActive(long[] counts) {
        String[] dates = new String[3];
        dates[0] = DateManager.getDaysBefore(1, 0);     //2014-08-03
        dates[1] = DateManager.getDaysBefore(8, 0);     //2014-07-27
        dates[2] = DateManager.getDaysBefore(31, 0);    //2014-07-04
        String key = "";
        for(int i = 0; i < 3; i++) {
            key = "COMMON," + specialTask + "," + dates[i] + "," + dates[0] + ",visit.*,TOTAL_USER,VF-ALL-0-0,PERIOD";
            keyList.add(key);
        }

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

    /**
     * 保存一周留存率
     * @param ret
     */
    public void storeRetention(long ret) {
        Map<String, Number[]> result = new HashMap<String, Number[]>();
        String key = "COMMON,internet-1,2014-08-02,2014-08-07,visit.*,{\"register_time\":{\"$gte\":\"2014-08-01\",\"$lte\":\"2014-08-01\"}},VF-ALL-0-0,PERIOD";
        result.put(key, new Number[]{0, 0, ret, 1.0});
        XCacheOperator xCacheOperator = RedisXCacheOperator.getInstance();
        try {
            MapXCache xCache = MapXCache.buildMapXCache(key, result);
            xCacheOperator.putMapCache(xCache);
        } catch (XCacheException e) {
            e.printStackTrace();
        }

    }

    /**
     * 保存每日新增用户数
     * @param count
     */
    public void storeNewUserNum(long count) {
        String date = DateManager.getDaysBefore(6, 0);
        Map<String, Number[]> result = null;
        MapXCache xCache = null;
        XCacheOperator xCacheOperator = RedisXCacheOperator.getInstance();
        String key = "";
        try {
            key = "COMMON," + specialTask + "," + date + "," + date + ",visit.*,{\"register_time\":{\"$gte\":\"" + date + "\",\"$lte\":\"" + date + "\"}},VF-ALL-0-0,PERIOD";
            result = new HashMap<String, Number[]>();
            result.put(key, new Number[]{0, 0, count, 1.0});
            xCache = MapXCache.buildMapXCache(key, result);
            xCacheOperator.putMapCache(xCache);
        } catch (XCacheException e) {
            e.printStackTrace();
        }

    }

    /**
     * test
     * @param counts
     */
    public void testStore(long counts) {
        Map<String, Number[]> result = null;
        MapXCache xCache = null;
        String key = "COMMON,internet-1,2014-07-04,2014-08-03,visit.*,TOTAL_USER,VF-ALL-0-0,PERIOD";
        XCacheOperator xCacheOperator = RedisXCacheOperator.getInstance();
        try {
                result = new HashMap<String, Number[]>();
                result.put(key, new Number[]{0, 0, counts, 1.0});
                xCache = MapXCache.buildMapXCache(key, result);
                xCacheOperator.putMapCache(xCache);
        } catch (XCacheException e) {
            e.printStackTrace();
        }
    }


}
