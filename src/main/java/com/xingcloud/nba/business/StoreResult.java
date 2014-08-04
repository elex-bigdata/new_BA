package com.xingcloud.nba.business;

import com.xingcloud.maincache.MapXCache;
import com.xingcloud.maincache.XCacheException;
import com.xingcloud.maincache.redis.RedisXCacheOperator;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.*;

/**
 * Created by Administrator on 14-8-4.
 */
/**
    [
        {
        "items": [
        {
        "name": "x",
        "event_key": "visit.*.*.*.*.*",
        "count_method": "USER_NUM",
        "scale": "1970-01-01:1.0",
        "number_of_day": 0,
        "number_of_day_origin": 0,
        "segmentv2": ""
        }
        ],
        "id": "m695s0",
        "end_time": "2014-08-03",
        "start_time": "2014-07-28",
        "interval": "DAY",
        "type": "COMMON",
        "project_id": "internet-1"
        }
    ]
 COMMON,age,2014-04-28,2014-05-28,visit.*,TOTAL_USER,VF-ALL-0-0,PERIOD
 */
public class StoreResult {
    private static Log LOG = LogFactory.getLog(StoreResult.class);
    // query type
    public static final byte QUERY_TYPE_COMMON = 0;

    // interval (if only type == QUER_TYPE_COMMON)
    public static final byte INTERVAL_MIN5 = 0;
    public static final byte INTERVAL_HOUR = 1;
    public static final byte INTERVAL_PERIOD = 2; // day

    public static final String QUERY_TYPE_COMMON_STRING = "COMMON";
    public static final String GROUP_BY_EVENT_STRING = "EVENT";
    public static final String GROUP_BY_USERPROPERTY_STRING = "USER_PROPERTIES";
    public static final String INTERVAL_MIN5_STRING = "MIN5";
    public static final String INTERVAL_HOUR_STRING = "HOUR";
    public static final String INTERVAL_PERIOD_STRING = "PERIOD";
    public static final char COMMA = ',';

    // fixedField
    public static final String fixedField = "VF-ALL-0-0";

    // key
    private byte query_type;
    private String pid = null;
    private String beginDate = null;
    private String endDate = null;
    private String event = null;
    private TreeMap<String, Object> segment = new TreeMap<String, Object>();
    private byte interval;
    private byte gbt;
    private String gbv = null;

    // values
    private Map<String, long[]> keyValues = new HashMap<String, long[]>();





    public void store(long wac) {
        Map<String, Number[]> result = new HashMap<String, Number[]>();
        String key = "COMMON,internet-1,2014-08-03,2014-08-03,visit.*,TOTAL_USER,VF-ALL-0-0,PERIOD";
        result.put(key, new Number[]{0, 0, wac, 1.0});
        MapXCache xCache = null;
        try {
            xCache = MapXCache.buildMapXCache(key, result);
            RedisXCacheOperator.getInstance().putMapCache(xCache);
        } catch (XCacheException e) {
            e.printStackTrace();
        }
    }




























    // generate key for record to store in redis
    /*public String getRecordKey() {
        StringBuffer recordKey = new StringBuffer(100);
        if (query_type == QUERY_TYPE_COMMON) {
            recordKey.append(Record.QUERY_TYPE_COMMON_STRING);
        } else {
            recordKey.append(Record.QUERY_TYPE_GROUP_STRING);
        }
        recordKey.append(COMMA)
                .append(pid)
                .append(COMMA)
                .append(beginDate)
                .append(COMMA)
                .append(endDate)
                .append(COMMA)
                .append(event)
                .append(COMMA);
        if (segment.isEmpty())
            recordKey.append(Constant.TOTAL_USER_IDENTIFIER);
        else
            recordKey.append(gson.toJson(segment));
        recordKey.append(COMMA);
        recordKey.append(Record.fixedField);
        recordKey.append(COMMA);
        if (query_type == QUERY_TYPE_COMMON) {
            if (interval == Record.INTERVAL_MIN5)
                recordKey.append(Record.INTERVAL_MIN5_STRING);
            else if (interval == Record.INTERVAL_HOUR)
                recordKey.append(Record.INTERVAL_HOUR_STRING);
            else
                recordKey.append(Record.INTERVAL_PERIOD_STRING);
        } else {
            if (gbt == Record.GROUP_BY_EVENT)
                recordKey.append(Record.GROUP_BY_EVENT_STRING);
            else
                recordKey.append(Record.GROUP_BY_USERPROPERTY_STRING);
            recordKey.append(COMMA);
            recordKey.append(gbv);
        }

        return recordKey.toString();
    }*/
}
