package com.atguigu.mr;

import com.atguigu.kv.key.CommonDimension;
import com.atguigu.kv.value.CountDurationValue;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CountDurationReducer extends Reducer<CommonDimension,Text,CommonDimension,CountDurationValue> {
    private CountDurationValue countDurationValue = null;
    @Override
    protected void reduce(CommonDimension key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        //通话时间和通话时长
        int countSum = 0;
        int durationSum = 0;

        //统计通话时长及通话次数
        for (Text value : values) {
            String durationString = value.toString();
            int durationInt = Integer.parseInt(durationString);
            durationSum += durationInt;
            countSum++;
        }

        //设置值
        countDurationValue = new CountDurationValue();
        countDurationValue.setCount(countSum);
        countDurationValue.setDuration(durationSum);

        //写出去
        context.write(key,countDurationValue);
    }
}
