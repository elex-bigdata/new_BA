package com.elex.hive.udf;

import com.xingcloud.util.Base64Util;
import com.xingcloud.util.HashFunctions;
import com.xingcloud.xa.uidmapping.UidMappingUtil;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Author: liqiang
 * Date: 14-8-28
 * Time: 上午10:34
 */
public class MD5UID  extends UDF {

    public String evaluate(String struid){
        try{
            long uid = Long.parseLong(struid);
            return String.valueOf(UidMappingUtil.getInstance().decorateWithMD5(uid));
        }catch(Exception e){
            return "";
        }
    }

}
