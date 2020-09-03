/*
package com.yyx.bigdata.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;


*/
/**
 * @author Aaron-yang
 * @date 2020/7/8 15:16
 *//*

public class RenameHomework {

    FileSystem fileSystem;

    private final static String DIR_PATH = "/user/hadoop/yyx-hdfs/";

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

    */
/**
     * 造数据
     * @throws Exception
     *//*

    @Test
    public void testMakeData () throws Exception {
        //创建文件夹
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        final String dateString = dateFormat.format(new Date());
        Path path = new Path(DIR_PATH + dateString);
        fileSystem.mkdirs(path);

        //上传文件
        Path srcPath1 = new Path("out/1.txt");
        fileSystem.copyFromLocalFile(srcPath1, path);

        Path srcPath2 = new Path("out/2.txt");
        fileSystem.copyFromLocalFile(srcPath2, path);
    }

    */
/**
     * 测试入口
     * @throws Exception
     *//*

    @Test
    public void testRenameFileByDate () throws Exception {
        renameFileByDate(new Date());
    }

    private void renameFileByDate(Date date) throws Exception{

        //获得符合条件的日期字符串
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        final String dateString = dateFormat.format(date);
        Path path = new Path(DIR_PATH + dateString);

        //根据日期验证文件夹是否存在
        boolean isDirectory = fileSystem.isDirectory(path);
        if(!isDirectory){
            return;
        }

        //遍历重命名文件夹下面的文件
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(path, false);
        Integer orderNumber = 0;
        while (files.hasNext()){
            LocatedFileStatus fileStatus = files.next();

            //文件原路径
            Path filePath = fileStatus.getPath();
            System.out.println(filePath);

            //重命名
            String newFileName = dateString + "-" + orderNumber + ".txt";
            Path newFilePath = new Path(DIR_PATH + dateString + "/" + newFileName);
            fileSystem.rename(filePath, newFilePath);
            orderNumber ++;
        }

    }

}
*/
