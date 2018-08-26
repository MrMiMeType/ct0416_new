package com.atguigu.utils;
import java.sql.*;

public class JDBCUtil {

    //连接数据库必须驱动、url、username、password
    private static final String  MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    private static final String  MYSQL_URL = "jdbc:mysql://hadoop102:3306/ct0416?useUnicode=true&characterEncoding=UTF-8";
    private static final String  MYSQL_USERNAME = "root";
    private static final String  MYSQL_PASSWORD = "111111";
    private static Connection connection;

    //获取连接的方法
    private static Connection getConnection(){
        try {
            //获取驱动类
            Class.forName(MYSQL_DRIVER);

            //获取连接
            connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USERNAME, MYSQL_PASSWORD);
        } catch (ClassNotFoundException| SQLException e) {
            e.printStackTrace();
        }
        return connection;
    }

    //关闭连接的方法
    public static void close(Connection connection, PreparedStatement preparedStatement, ResultSet resultSet){
        if (resultSet != null){
            try {
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (preparedStatement != null){
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        if (connection != null){
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    //返回连接的方法
    public static Connection getInstance(){
        if(connection == null){
            synchronized (JDBCUtil.class){
                if(connection == null){
                    connection = JDBCUtil.getConnection();
                }
            }
        }
        return connection;
    }
}
