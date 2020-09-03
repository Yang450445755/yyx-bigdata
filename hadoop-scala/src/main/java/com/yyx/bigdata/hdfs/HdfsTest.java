package com.yyx.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.net.URI;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * @author Aaron-yang
 * @date 2020/7/7 10:25
 */
public class HdfsTest {

    private FileSystem fileSystem;


    public static void main(String[] args) throws Exception {
        //创建得到fileSystem
        Configuration configuration = new Configuration();
        configuration.set("dfs.client.use.datanode.hostname","true");
        configuration.set("dfs.replication","1");
        URI uri = new URI("hdfs://yyxdata001:9000");
        FileSystem fileSystem = FileSystem.get(uri,configuration,"hadoop");

        //创建文件夹
        Path path = new Path("/yyx");
        RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(path, true);
        while (iterator.hasNext()){
            LocatedFileStatus status = iterator.next();

            System.out.println(status);
        }

        fileSystem.close();
    }

   /* @Before
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
    }*/
}
