package com.atguigu.output;


import com.atguigu.convertor.DimensionConvertor;
import com.atguigu.kv.key.CommonDimension;
import com.atguigu.kv.key.ContactDimension;
import com.atguigu.kv.key.DateDimension;
import com.atguigu.kv.value.CountDurationValue;
import com.atguigu.utils.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class MysqlOutputFormat extends OutputFormat<CommonDimension, CountDurationValue> {

    private OutputCommitter outputCommitter = null;



    @Override
    public RecordWriter<CommonDimension, CountDurationValue> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MySQLRecordWriter();
    }

    //校验输出的方法
    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    //outputCommitter并没有用到啊，可不可以直接返回null?
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        if (outputCommitter == null){
            Path outPutPath = getOutPutPath(taskAttemptContext);
            outputCommitter = new FileOutputCommitter(outPutPath, taskAttemptContext);
        }
        return outputCommitter;
    };

    public static Path getOutPutPath(JobContext job){
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }


    public static class MySQLRecordWriter extends RecordWriter<CommonDimension, CountDurationValue>{

        //属性私有化
        private DimensionConvertor dimensionConvertor = null;
        private String sql = null;
        private int id_contact = 0;
        private int id_date_dimension = 0;
        private Connection connection = null;
        PreparedStatement preparedStatement =null;


        @Override
        public void write(CommonDimension commonDimonsion, CountDurationValue countDurationValue) throws IOException, InterruptedException {
            
            //获取5个值
            int callCount = countDurationValue.getCount();
            int callDuration = countDurationValue.getDuration();

            //去mysql表查询数据,获取id值
            ContactDimension contactDimonsion = commonDimonsion.getContactDimonsion();
            DateDimension dateDimonsion = commonDimonsion.getDateDimonsion();
            try {
                id_contact = dimensionConvertor.getDimendionID(contactDimonsion);
                id_date_dimension = dimensionConvertor.getDimendionID(dateDimonsion);
            } catch (SQLException e) {
                e.printStackTrace();
            }

            //拼接主键
            String primaryKey = id_contact +"_"+ id_date_dimension;

            //编写sql,有则更新无则插入
            String sql = "insert into tb_call values (?,?,?,?,?) on duplicate key update call_sum = ?,call_duration_sum = ?;";


            //添加到缓存
            
            //写到mysql

            //关闭资源

            
        }

        //构造方法，用于初始化属性
        public MySQLRecordWriter() {
            dimensionConvertor = new DimensionConvertor();
            sql = "insert into tb_call values (?,?,?,?,?) on duplicate key update call_sum = ?,call_duration_sum = ?;";
            connection = JDBCUtil.getInstance();
            try {
                connection.setAutoCommit(false);
                preparedStatement = connection.prepareStatement(sql);
            } catch (SQLException e) {
                e.printStackTrace();
            }

            //批量提交控制参数
            int cacheBount = 500;
            int count = 0;

        }

        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        }
    }
}
