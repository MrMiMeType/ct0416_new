import com.atguigu.constant.Constant;
import com.atguigu.utils.HBaseScanUtil;
import com.atguigu.utils.PropertiesUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.util.List;

public class TestStartRowAndEndRow {
    public static void main(String[] args) throws ParseException, IOException {
        HBaseScanUtil hBaseScanUtil = new HBaseScanUtil();

        //获取连接
        Connection connection = ConnectionFactory.createConnection(Constant.CONF);

        //获取表对象
        Table table = connection.getTable(TableName.valueOf(PropertiesUtil.getProperty("hbase.table.name")));

        //获取startKeyAndendKey
        List<String[]> startAndEndKeys = hBaseScanUtil.getStartAndEndKeys("19879419704", "2017-09", "2018-02");

        //循环变量获取值
        for (String[] startAndEndKey : startAndEndKeys) {
            //获取Scan对象
            Scan scan = new Scan(Bytes.toBytes(startAndEndKey[0]),Bytes.toBytes(startAndEndKey[1]));

            //获取扫描哦结果对象
            ResultScanner results = table.getScanner(scan);

            //获取rowKey

            //rowKey为：05_19879419704_2017-09-28 12:25:04_19379884788_1_1751
            //rowKey为：00_19879419704_2017-10-03 17:50:41_15133295266_0_0012
            //rowKey为：00_19879419704_2017-10-10 23:03:13_18674257265_1_1034
            //rowKey为：00_19879419704_2017-10-19 17:21:21_15596505995_0_0615

            for (Result result : results) {
                System.out.println("rowKey为："+Bytes.toString(result.getRow()));
            }
        }
    }
}
