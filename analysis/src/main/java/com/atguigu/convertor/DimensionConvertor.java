package com.atguigu.convertor;

import com.atguigu.kv.base.BaseDimension;
import com.atguigu.kv.key.ContactDimension;
import com.atguigu.kv.key.DateDimension;
import com.atguigu.utils.JDBCUtil;
import com.atguigu.utils.LRUCache;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

//根据时间维度和联系人维度获取对应维度的id
public class DimensionConvertor {

    private LRUCache cache = new LRUCache(5000);
    private static Connection connection = JDBCUtil.getInstance();
    private ResultSet resultSet = null;

    public int getDimendionID(BaseDimension baseDimension) throws SQLException {
        String lruKey = getLRUKey(baseDimension);
        //读缓存数据
        if (cache.containsKey(lruKey)) {
            return cache.get(lruKey);
        }

        //获取对应维度的sql语句
        String[] sqls = getSQL(baseDimension);

        //获取主键值
        int id = execSql(connection, sqls, baseDimension);
        if(id ==0){
            throw new RuntimeException("未找到匹配的维度信息！");
        }

        //将获取的结果写入缓存中
        cache.put(lruKey,id);

        //返回
        return id;
    }

    //判断baseDimension是什么类型，获取telephone/yearmonthday字段，然后获取主键
    public String getLRUKey(BaseDimension baseDimension){
        StringBuilder stringBuilder = new StringBuilder();
        if(baseDimension instanceof ContactDimension){
            ContactDimension contactDimension = (ContactDimension) baseDimension;
            stringBuilder.append(contactDimension.getTelephone());
        } else {
            DateDimension dateDimension = (DateDimension) baseDimension;
            stringBuilder.append(dateDimension.getYear()).append(dateDimension.getMonth()).append(dateDimension.getDay());
        }
        return stringBuilder.toString();
    }

    //编写sql的方法
    public String[] getSQL(BaseDimension baseDimension){
        String[] sqls = new String[2];
        if(baseDimension instanceof ContactDimension){
            //为了有则查询无则插入的原则，所以要编写查询和插入的sql
            sqls[0]= "select id from tb_contacts where telephone = ? and name = ?;";
            sqls[1]= "insert into tb_contacts values (null,?,?);";
        } else {
            sqls[0]= "select id from tb_dimension_date where year = ? and month = ? and day = ?;";
            sqls[1]= "insert into tb_dimension_date values (null,?,?,?);";
        }
        return sqls;
    }

    //给sql设置值的方法
    public void setArguments(PreparedStatement preparedStatement,BaseDimension baseDimension) throws SQLException {
        int index = 0;
        if(baseDimension instanceof ContactDimension){
            ContactDimension contactDimension = (ContactDimension) baseDimension;
            preparedStatement.setString(++index,contactDimension.getTelephone());
            preparedStatement.setString(++index,contactDimension.getName());
        } else {
            DateDimension dateDimension = (DateDimension) baseDimension;
            preparedStatement.setInt(++index,Integer.parseInt(dateDimension.getYear()));
            preparedStatement.setInt(++index,Integer.parseInt(dateDimension.getMonth()));
            preparedStatement.setInt(++index,Integer.parseInt(dateDimension.getDay()));
        }
    }

    //执行sql获取contactDimension/dateDimension主键的方法
    private int execSql(Connection connection, String[] sqls, BaseDimension baseDimension) throws SQLException {

        PreparedStatement preparedStatement = null;

        //第一次查询
        preparedStatement = connection.prepareStatement(sqls[0]);
        setArguments(preparedStatement, baseDimension);
        if (preparedStatement.execute()){
            resultSet = preparedStatement.getResultSet();
            return resultSet.getInt("1");
        }

        //插入操作
        preparedStatement = connection.prepareStatement(sqls[1]);
        setArguments(preparedStatement, baseDimension);
        preparedStatement.executeUpdate();

        //第二次查询
        preparedStatement = connection.prepareStatement(sqls[0]);
        setArguments(preparedStatement, baseDimension);
        if (preparedStatement.execute()){
           resultSet = preparedStatement.getResultSet();
            return resultSet.getInt("1");
        }

        //关闭资源
        JDBCUtil.close(null,preparedStatement,resultSet);

        return 0;
    }
}
