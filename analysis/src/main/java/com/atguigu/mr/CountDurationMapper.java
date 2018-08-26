package com.atguigu.mr;

import com.atguigu.kv.key.CommonDimension;
import com.atguigu.kv.key.ContactDimension;
import com.atguigu.kv.key.DateDimension;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Map;

//
public class CountDurationMapper extends TableMapper<CommonDimension,Text> {
    //将要用到的对象全部声明在这儿到底可不可以？
    private Map contacts = null;
    private Text text = new Text();
    private CommonDimension commonDimonsion = null;


    //将姓名数据缓存在这份方法中，实际中应该是从数据库中去查询然后放在这儿！
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {

        //将姓名数据缓存在这份方法中，实际中应该是从数据库中去查询然后放在这儿！
        contacts.put("15369468720", "李雁");
        contacts.put("19920860202", "卫艺");
        contacts.put("18411925860", "仰莉");
        contacts.put("14473548449", "陶欣悦");
        contacts.put("18749966182", "施梅梅");
        contacts.put("19379884788", "金虹霖");
        contacts.put("19335715448", "魏明艳");
        contacts.put("18503558939", "华贞");
        contacts.put("13407209608", "华啟倩");
        contacts.put("15596505995", "仲采绿");
        contacts.put("17519874292", "卫丹");
        contacts.put("15178485516", "戚丽红");
        contacts.put("19877232369", "何翠柔");
        contacts.put("18706287692", "钱溶艳");
        contacts.put("18944239644", "钱琳");
        contacts.put("17325302007", "缪静欣");
        contacts.put("18839074540", "焦秋菊");
        contacts.put("19879419704", "吕访琴");
        contacts.put("16480981069", "沈丹");
        contacts.put("18674257265", "褚美丽");
        contacts.put("18302820904", "孙怡");
        contacts.put("15133295266", "许婵");
        contacts.put("17868457605", "曹红恋");
        contacts.put("15490732767", "吕柔");
        contacts.put("15064972307", "冯怜云");

    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //获取rowk，并拆分
        String rowKey = Bytes.toString(value.getRow());
        String[] splits = rowKey.split("_");

        //只获取主叫数据
        if ("1".equals(splits[4])){
            return ;
        }

        //封装属性
        String caller = splits[1];
        //2010-03-10
        String buildTime = splits[2];
        String year = buildTime.substring(0,4);
        String month = buildTime.substring(5,7);
        String day = buildTime.substring(8,10);
        String callee = splits[3];
        String duration = splits[5];

        //主叫数据联系人维度
        ContactDimension callerContactDimonsion = new ContactDimension();
        String callerName = (String )contacts.get(caller);
        callerContactDimonsion.setTelephone(caller);
        callerContactDimonsion.setName(callerName);

        //被叫数据联系人维度
        ContactDimension calleeContactDimonsion = new ContactDimension();
        callerContactDimonsion.setTelephone(callee);
        String calleeName = (String )contacts.get(callee);
        calleeContactDimonsion.setName(calleeName);

        //时间维度
        DateDimension dateDimonsionYear = new DateDimension(year,"-1","-1");
        DateDimension dateDimonsionMonth = new DateDimension(year,month,"-1");
        DateDimension dateDimonsionDay = new DateDimension(year,month,day);

        //设置写出去的value
        text.set(duration);

        //封装key并写出去
        //主叫
        commonDimonsion = new CommonDimension();
        commonDimonsion.setContactDimonsion(callerContactDimonsion);

        //设置day
        commonDimonsion.setDateDimonsion(dateDimonsionDay);
        context.write(commonDimonsion,text);

        //设置month
        commonDimonsion.setDateDimonsion(dateDimonsionMonth);
        context.write(commonDimonsion,text);

        //设置year
        commonDimonsion.setDateDimonsion(dateDimonsionYear);
        context.write(commonDimonsion,text);

        //被叫
        commonDimonsion.setContactDimonsion(calleeContactDimonsion);

        //设置day
        commonDimonsion.setDateDimonsion(dateDimonsionDay);
        context.write(commonDimonsion,text);

        //设置month
        commonDimonsion.setDateDimonsion(dateDimonsionMonth);
        context.write(commonDimonsion,text);

        //设置year
        commonDimonsion.setDateDimonsion(dateDimonsionYear);
        context.write(commonDimonsion,text);


    }
}
