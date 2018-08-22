package com.atguigu.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class HBaseScanUtil {

    //获取分区数
    private int regions = Integer.parseInt(PropertiesUtil.getProperty("hbase.regions"));

    //获取开始和结束的键的方法
    public List<String[]> getStartAndEndKeys(String phoneNumber, String start, String end) throws ParseException {
        ArrayList<String[]> list = new ArrayList<>();

        ///获取日历类
        Calendar startPoint = Calendar.getInstance();
        Calendar endPoint = Calendar.getInstance();

        //设置开始时间和结束时间
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM");
        Date startDate = simpleDateFormat.parse(start);
        Date endDate = simpleDateFormat.parse(end);
        startPoint.setTime(startDate);
        endPoint.setTime(endDate);

        //每个月的结束标志
        Calendar monthEndPoint = Calendar.getInstance();
        monthEndPoint.setTime(startDate);
        monthEndPoint.add(Calendar.MONTH,1);

        //获取每月的startKey和endKey
        while(startPoint.getTime().getTime() <= endPoint.getTime().getTime()){
            String[] arrays = new String[2];

            //将每月的开始时间，结束时间转化为字符串
            String startTime = simpleDateFormat.format(startPoint.getTime());
            String endTime = simpleDateFormat.format(monthEndPoint.getTime());

            //获取分区号,拼接startKey和endKey,设置到数组中
            String partitionID = HBaseUtil.getPartitionID(phoneNumber, startTime, regions);
            arrays[0] = partitionID + "_" + phoneNumber + "_" + startTime;
            arrays[1] = partitionID + "_" + phoneNumber + "_" + endTime;

            //设置到集合中
            list.add(arrays);

            //设置开始和结束的时间点
            startPoint.add(Calendar.MONTH,1);
            monthEndPoint.add(Calendar.MONTH,1);
        }

        //返回结果
        return list;
    }
}
