package com.yyx.hadfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URI;

/**
 * @author Aaron-yang
 * @date 2020/7/7 14:16
 */
public class HdfsDownloadTest {

    private FileSystem fileSystem;

    @Before
    public void setUp() throws Exception{
        //创建得到fileSystem
        Configuration configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname","true");
        configuration.set("dfs.replication","1");
        URI uri = new URI("hdfs://yyxdata001:9000");
        fileSystem = FileSystem.get(uri,configuration,"hadoop");
    }


    @Test
    public void testDownload1 () throws Exception {
        FSDataInputStream in = fileSystem.open(new Path("/user/hadoop/yyx-hdfs/nbnbnb.txt"));
        FileOutputStream out = new FileOutputStream(new File("out/1.txt"));

        byte[] buffer = new byte[1024];
        int readResult;

        //-1就表示没有数据了
        while ((readResult = in.read(buffer)) != -1) {
            System.out.println(readResult);
            out.write(buffer);
        }
        out.write(buffer);
        System.out.println(readResult);

        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }

    @Test
    public void testDownload2 () throws Exception {
        FSDataInputStream in = fileSystem.open(new Path("/user/hadoop/yyx-hdfs/nbnbnb.txt"));
        FileOutputStream out = new FileOutputStream(new File("out/2.txt"));

        in.seek(1);
        IOUtils.copyBytes(in, out, fileSystem.getConf());

        IOUtils.closeStream(in);
        IOUtils.closeStream(out);
    }
}
