package com.atguigu.hbaseconsumer;

import com.atguigu.utils.PropertiesUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Collections;
//测试从flume->kafka通了没有！
public class HbaseConsumer {

    public static void main(String[] args) throws IOException {

        //创建Kafka消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(PropertiesUtil.properties);

        //订阅指定topic
        consumer.subscribe(Collections.singletonList(PropertiesUtil.getProperty("kafka.topics")));

        //往表中添加数据
        HBaseDAO hbaseDAO = new HBaseDAO();
        try {
            while (true) {
                //100ms获取一次topic数据
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String > record : records) {
                    String line = record.value();
                    System.out.println(line);
                    //存在部分数据未被消费！
                    hbaseDAO.puts(line);
                }
            }
        } finally {
            hbaseDAO.close();
        }
    }
}