package com.yyx.bigdata.bigdata;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.Random;

public class MockData {
    public static void main(String[] args) throws Exception{

        Random random = new Random();
        //时间数据
        String[] timeData = {"[9/Jun/2015:01:58:09 +0800]","[9/Jun/2016:01:58:09 +0800]","[9/Jun/2012:02:58:09 +0800]"};
        //ip数据
        String[] ipData = {"192.168.15.75","61.128.252.73","61.129.33.0"};
        //responsesize脏数据  20个
        String[] responsesizeData = {"243234"," ","-","2","laocao","max","yuanyuan","laohu","jeep","rookie","xiaoyang","monkey","1234","2830","324","3256","75783","2754","9896896","54223"};
        BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(
                        new FileOutputStream(new File("data/mockdata.txt"))
                )
        );
        for (int i=0;i <= 1000;i++){
            for (int j=0;j<= 0;j++){
                writer.write(timeData[random.nextInt(3)]+" "+ipData[random.nextInt(3)]+" "+"- 1542 \"-\" \"GET http://www.aliyun.com/index.html\" 200 191"+" "+responsesizeData[random.nextInt(20)]+" "+"MISS \"Mozilla/5.0 (compatible; AhrefsBot/5.0; +http://ahrefs.com/robot/)\" \"text/html\"");
                //writer.write(",");
            }
            writer.newLine();
        }
        writer.flush();
        writer.close();
    }
}
