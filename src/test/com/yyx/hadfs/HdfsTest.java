package com.yyx.hadfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

/**
 * @author Aaron-yang
 * @date 2020/7/7 10:25
 */
public class HdfsTest {

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

    @After
    public void close() throws Exception{
        fileSystem.close();
    }


    @Test
    public void testMkdir () throws Exception {
        //创建文件夹
        Path path = new Path("yyx-hdfs");
        fileSystem.mkdirs(path);
    }

    @Test
    public void testCopyFromLocal () throws Exception {
        Path srcPath = new Path("yyxnb.txt");
        Path dst = new Path("/user/hadoop/yyx-hdfs");
        fileSystem.copyFromLocalFile(srcPath, dst);
    }


    @Test
    public void testRename () throws Exception {
        Path originPath = new Path("/user/hadoop/yyx-hdfs/yyxnb.txt");
        Path currentPath = new Path("/user/hadoop/yyx-hdfs/nbnbnb.txt");
        fileSystem.rename(originPath, currentPath);
    }
}
