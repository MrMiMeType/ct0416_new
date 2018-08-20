import com.atguigu.constant.Constant;
import com.atguigu.utils.HBaseFilterUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class TestFileter {

    //查询15178485516在2017-01--2017-06的通话记录
    public static void main(String[] args) throws IOException {

        //创建过滤器
        Filter filter1 = HBaseFilterUtil.eqFilter("f1", "call1", Bytes.toBytes("15178485516"));
        Filter filter2 = HBaseFilterUtil.eqFilter("f1", "call2", Bytes.toBytes("15178485516"));
        Filter filter3 = HBaseFilterUtil.orFilter(filter1, filter2);
        Filter filter4 = HBaseFilterUtil.gteqFilter("f1","buildTime",Bytes.toBytes("2017-01"));
        Filter filter5 = HBaseFilterUtil.gteqFilter("f1","buildTime",Bytes.toBytes("2017-06"));
        Filter filter6 = HBaseFilterUtil.andFilter(filter4, filter5);
        Filter filter = HBaseFilterUtil.andFilter(filter3, filter6);

        //创建连接，获取表
        Connection connection = ConnectionFactory.createConnection(Constant.CONF);
        Table calllog = connection.getTable(TableName.valueOf("ns_telecom:calllog"));

        //创建Scan
        Scan scan = new Scan();
        Scan scan1 = scan.setFilter(filter);
        ResultScanner results = calllog.getScanner(scan1);
        for (Result result : results) {
            System.out.println("rowkey为："+Bytes.toString(result.getRow()));
        }
    }
}
