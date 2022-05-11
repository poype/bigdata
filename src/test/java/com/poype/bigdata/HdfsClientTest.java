package com.poype.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClientTest {

    @Test
    public void testMkdirs() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取文件句柄
        Configuration configuration = new Configuration();
        URI uri = new URI("hdfs://hadoop102:8020"); // NameNode的地址
        FileSystem fs = FileSystem.get(uri, configuration,"atguigu");

        // 2 创建目录
        fs.mkdirs(new Path("/xiyou/huaguoshan2/"));

        // 3 关闭资源
        fs.close();
    }
}
