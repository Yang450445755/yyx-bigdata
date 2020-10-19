package com.yyx.bigdata.bigdata.hive;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * @author Aaron-yang
 * @date 2020/7/23 10:36
 */
public class JDBCApp {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) throws Exception {
            Class.forName(driverName);
        Connection con = DriverManager.getConnection("jdbc:hive2://http://39.99.181.119:10000/bigdata", "hadoop", "");
        Statement stmt = con.createStatement();
        String tableName = "emp";
        String sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getInt("website_traffic") + "\t" + res.getString("domain"));
        }
    }
}
