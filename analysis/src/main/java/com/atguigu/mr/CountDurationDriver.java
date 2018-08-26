package com.atguigu.mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;

public class CountDurationDriver extends Configuration implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        return 0;
    }

    @Override
    public void setConf(Configuration configuration) {

    }

    @Override
    public Configuration getConf() {
        return null;
    }

    public static void main(String[] args) {

    }
}
