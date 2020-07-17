package com.yyx.bigdata.mr;

import com.yyx.bigdata.util.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Aaron-yang
 * @date 2020/7/9 15:05
 */
public class WordCountDriver3 {

    public static void main(String[] args) throws Exception {
        String input = "2.txt";
        String output = "out";

        // 1 获取Job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);


        FileUtils.deleteOutput(configuration, output);

        // 2 设置主类
        job.setJarByClass(WordCountDriver3.class);

        // 3 设置Mapper和Reducer
        job.setMapperClass(MyMapper1.class);
        job.setReducerClass(MyReducer1.class);

        // 4 设置Mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 5 设置Reduce阶段输出的key和value类型
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 7 提交Job
        boolean result = job.waitForCompletion(true);
        System.exit(result? 0:1 );
    }

    public static class MyMapper1 extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // 获取每一行的数据，按照指定分隔符进行拆分
            String[] splits = value.toString().split(" ");

            for (String split : splits) {
                double random = Math.random();
                int randomNum = (int)(random * 10);

                context.write(new Text(randomNum + "_" + split), new LongWritable(1));
            }

        }

        public MyMapper1() {
        }
    }



    public static class MyReducer1 extends Reducer<Text, LongWritable,Text, LongWritable> {

        @Override
        protected void reduce(Text text, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            int count = 0;

            for (LongWritable value : values) {
                count +=value.get();
            }
            context.write(text, new LongWritable(count));
        }

        public MyReducer1() {
        }
    }
}
