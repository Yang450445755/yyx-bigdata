package com.yyx.bigdata.bigdata;


import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Random;

/**
 * @author : Crazy
 * @date : 2020-08-17 23:54
 **/

public class CreateTestData {
    public static void main(String[] args) throws Exception{
        FileOutputStream fos = new FileOutputStream("data/MockData.txt");
        OutputStreamWriter ow = new OutputStreamWriter(fos);
        BufferedWriter writer = new BufferedWriter(ow);
        /**
         * @Description
         * [9/Jun/2015:01:58:09 +0800]	    日志开始时间
         * 192.168.15.75	                访问IP
         * 1542	                            responsetime（单位：ms）响应时间
         * "-"	                            referer
         * GET	                            method
         * http://www.aliyun.com/index.html	访问url
         * 200	                            httpcode
         * 191	                            requestsize（单位：byte）
         * 2830	                            responsesize（单位：byte）
         * MISS	                            cache命中状态
         * Mozilla/5.0（compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/）	UA头
         * text/html	                    文件类型
         */
        for(int i=0; i < 500; i++){
            String data = TestDate() + TestIp() + TestProxyIp() +TestRandom() + TestReferer() +TestReqMet() + TestWww()
                    + Teststatus() + TestRandom() + TestRandomDirty() + TestisMiss() +
                    "\"Mozilla/5.0 (compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/)\" \"text/html\"";
            writer.write(data);
            writer.newLine();
        }

        writer.flush();
        writer.close();

    }

    /**
     *@Description 时间范围
     */
    public static String TestDate() throws Exception{
        Locale.setDefault(Locale.US);
        long num = (long)(Math.random()*(1597334400000l - 1597248000000l)+1597248000000l);
        Date date= new Date(num);
        SimpleDateFormat sdf = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss +0800]");
        return sdf.format(date) + "\t";
    }

    /**
     *@Description IP范围
     */
    public static String TestIp() {
        int[][] range = {
                {607649792, 608174079}, // 36.56.0.0-36.63.255.255
                {1038614528, 1039007743}, // 61.232.0.0-61.237.255.255
                {1783627776, 1784676351}, // 106.80.0.0-106.95.255.255
                {2035023872, 2035154943}, // 121.76.0.0-121.77.255.255
                {2078801920, 2079064063}, // 123.232.0.0-123.235.255.255
                {-1950089216, -1948778497}, // 139.196.0.0-139.215.255.255
                {-1425539072, -1425014785}, // 171.8.0.0-171.15.255.255
                {-1236271104, -1235419137}, // 182.80.0.0-182.92.255.255
                {-770113536, -768606209}, // 210.25.0.0-210.47.255.255
                {-569376768, -564133889}, // 222.16.0.0-222.95.255.255
        };
        Random random = new Random();
        int index = random.nextInt(10);
        int ip = range[index][0] + new Random().nextInt(range[index][1] - range[index][0]);
        int[] b = new int[4];
        String ipStr = "";
        b[0] = ((ip >> 24) & 0xff);
        b[1] = ((ip >> 16) & 0xff);
        b[2] = ((ip >> 8) & 0xff);
        b[3] = (ip & 0xff);
        ipStr = Integer.toString(b[0]) + "." + Integer.toString(b[1]) + "." + Integer.toString(b[2]) + "." + Integer.toString(b[3]);
        return ipStr + "\t";
    }
    /**
     *
     * proxyip
     */
    public static String TestProxyIp() {
        return "-" +"\t";
    }
    /**
     *@Description responsetime、requestsize、responsesize
     */
    public static String TestRandom(){
        Random rd = new Random();
        return Integer.toString(rd.nextInt(2000)) + "\t";
    }

    /**
     *@Description responsetime脏数据
     */
    public static String TestRandomDirty(){
        Random rd = new Random();
        if ((int)(Math.random()*10) == 0 ) return "-" + "\t";
        else return Integer.toString(rd.nextInt(2000)) + "\t";
    }

    /**
     *@Description 请求方式
     */
    public static String TestReqMet(){
        if((int)(Math.random()*2) == 0 ){
            return "\"GET" + "\t";
        }else return "POST" + "\t";
    }

    /**
     *@Description 状态码
     */
    public static String Teststatus(){
        if((int)(Math.random()*2) == 0 ){
            return "200" + "\t";
        }else return "500" + "\t";
    }

    /**
     *@Description 网址
     */
    public static String TestWww(){
        String head[] = new String[]{"http://","https://"};
        String path[] = new String[]{"douyu.com\"","huya.com/293327\"","leetcode-cn.com?sl=zh-CN\"","baidu.com/?sl=zh-CN&tl=en&text=head\""};

        return head[(int)(Math.random()*2)] + "www." + path[(int)(Math.random()*4)] + "\t";
    }

    /**
     *@Description 命中率
     */
    public static String TestisMiss(){
        if((int)(Math.random()*2) == 0 ){
            return "Miss" + "\t";
        }else return "HIT" + "\t";
    }

    /**
     *@Description Referer
     */
    public static String TestReferer(){
        return "\"-\"" + "\t";
    }
}
