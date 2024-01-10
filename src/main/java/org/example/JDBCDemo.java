package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Hello world!
 *
 */
public class App {
    public static void main(String[] args) throws SQLException {
        Connection conn = null;
        try {
            Class.forName("ru.yandex.clickhouse.ClickHouseDriver");
            conn = DriverManager.getConnection("jdbc:clickhouse://clickhouse.lixingyu.cn:7123/test", "admin", "lxy123");
        } catch (Exception e) {
            System.out.println("获取数据库连接异常！");
        }
        PreparedStatement pstmt = conn.prepareStatement(
            "CREATE TABLE test.user_info (name VARCHAR(20),age UInt8,sex UInt8,id UInt8) ENGINE = MergeTree primary key id;");
        pstmt.execute();
        System.out.println("建表语句执行成功！");
        pstmt = conn.prepareStatement(
            "insert into user_info (name,age,sex) values('zhangsan', '20', '1'),('lisi', '100', '1'),('wangwu', '3', '0'),('zhaoliu', '20', '1')");
        pstmt.execute();
        System.out.println("插入语句执行成功！");
        List<Map<String, Object>> list = new ArrayList<>();
        String sql = "select * from user_info";
        try {
            Statement statement = conn.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            ResultSetMetaData rsmd = rs.getMetaData();
            while (rs.next()) {
                Map<String, Object> row = new HashMap<>();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    row.put(rsmd.getColumnName(i), rs.getObject(rsmd.getColumnName(i)));
                }
                list.add(row);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        list.forEach(item -> {
            System.out.println(item);
        });
        System.out.println("查询语句执行成功！");
        pstmt = conn.prepareStatement("alter table user_info update name='xxxx' where sex = '1'");
        pstmt.execute();
        System.out.println("修改语句执行成功！");
        list.clear();
        Statement statement = conn.createStatement();
        ResultSet rs = statement.executeQuery(sql);
        ResultSetMetaData rsmd = rs.getMetaData();
        while (rs.next()) {
            Map<String, Object> row = new HashMap<>();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                row.put(rsmd.getColumnName(i), rs.getObject(rsmd.getColumnName(i)));
            }
            list.add(row);
        }
        statement = conn.createStatement();
        list.forEach(item -> {
            System.out.println(item);
        });
        System.out.println("查询语句执行成功！");
        statement.execute("drop table user_info;");
        System.out.println("删除语句执行成功！");
    }
}
