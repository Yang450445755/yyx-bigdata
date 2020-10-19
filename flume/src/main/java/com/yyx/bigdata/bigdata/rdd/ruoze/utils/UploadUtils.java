package com.yyx.bigdata.bigdata.rdd.ruoze.utils;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * @author PK哥
 **/
public class UploadUtils {

    public static void upload(String path, String log)  {

        try {
            URL url = new URL(path);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();

            // GET POST
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);


            // json数据==>application/json
            connection.setRequestProperty("Content-Type","application/text");

            OutputStream out = connection.getOutputStream();
            out.write(log.getBytes());
            out.flush();
            out.close();

            int responseCode = connection.getResponseCode();
            System.out.println(responseCode);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
