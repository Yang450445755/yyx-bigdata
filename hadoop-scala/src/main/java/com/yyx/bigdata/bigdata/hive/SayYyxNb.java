package com.yyx.bigdata.bigdata.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author Aaron-yang
 * @date 2020/7/23 10:36
 */
@Description(name = "Top10",
        value = "_FUNC_(str) - Returns yyx nb!!! --> str",
        extended = "Example:\n" +
                    " aaa   -   yyx nb!!! --> aaa ")
public class SayYyxNb extends UDF {
    public String evaluate(String input) throws Exception{
        return "yyx nb!!! --> " + input;
    }


}
