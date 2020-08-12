package com.yyx.bigdata.hive;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.*;


@Description(name = "Top10",
        value = "_FUNC_(str) - Returns str : str",
        extended = "Example:\n"
                + "  > SELECT _FUNC_('http://ruozedata.com/course/983686.html') FROM src LIMIT 1;\n" + "  '983686'"
                + "  > SELECT _FUNC_('http://ruozedata.com/course/890196/2.html?a=b&c=d') FROM src LIMIT 1;\n" + "  '983686_2'")
public class Top extends UDF {
    public String evaluate(String input) throws Exception{
        //正则
        Pattern pattern = compile("(\\D*)(\\d+)(\\D*)(\\d*)(\\S*)");
        Matcher matcher = pattern.matcher(input);

        String result = "";
        if (matcher.find()) {
            String a = matcher.group(2);
            String b = matcher.group(4);
            if (!a.isEmpty()) {
                if (!b.isEmpty()) {
                    result = (a + "_" + b);
                } else {
                    result = a;
                }
            }
        }
        return result;
    }
}
