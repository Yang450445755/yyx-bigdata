package com.yyx.bigdata.mr;

import com.yyx.bigdata.util.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author Aaron-yang
 * @date 2020/7/9 15:05
 */
public class WordCountDriver3 {

    private static final String INPUT1 = "2.txt";
    private static final String OUTPUT1 = "out1";
    private static final String INPUT2 = "out1";
    private static final String OUTPUT2 = "out2";


    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();

        // 获取Job1
        Job job1 = getJob1(configuration);

        // 设置job1输入和输出路径
        FileInputFormat.setInputPaths(job1, new Path(INPUT1));
        FileOutputFormat.setOutputPath(job1, new Path(OUTPUT1));
        // 获取Job2
        Job job2 = getJob2(configuration);

        //job2输入和输出路径
        //输入路径是上一个作业的输出路径，因此这里填args[1],要和上面对应好
        FileInputFormat.addInputPath(job2, new Path(INPUT2));
        FileOutputFormat.setOutputPath(job2, new Path(OUTPUT2));

        //加入控制容器
        ControlledJob ctrlJob1 = new ControlledJob(configuration);
        ctrlJob1.setJob(job1);
        ControlledJob ctrlJob2 = new ControlledJob(configuration);
        ctrlJob2.setJob(job2);

        // 设置依赖关系
        ctrlJob2.addDependingJob(ctrlJob1);

        //主的控制容器，控制上面的总的两个子作业
        JobControl jobCtrl = new JobControl("myctrl");

        //添加到总的JobControl里，进行控制
        jobCtrl.addJob(ctrlJob1);
        jobCtrl.addJob(ctrlJob2);

        //在线程启动，记住一定要有这个
        Thread thread = new Thread(jobCtrl);
        thread.start();

        }

        private static Job getJob1 (Configuration configuration) throws Exception {
            Job job1 = Job.getInstance(configuration);

            FileUtils.deleteOutput(configuration, OUTPUT1);

            job1.setJarByClass(WordCountDriver3.class);

            job1.setMapperClass(MyMapper1.class);
            job1.setReducerClass(MyReducer1.class);

            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(LongWritable.class);

            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(LongWritable.class);
            return job1;
        }

        private static Job getJob2 (Configuration configuration) throws Exception {
            Job job2 = Job.getInstance(configuration);

            FileUtils.deleteOutput(configuration, OUTPUT2);

            job2.setJarByClass(WordCountDriver3.class);

            job2.setMapperClass(MyMapper2.class);
            job2.setReducerClass(MyReducer2.class);

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(LongWritable.class);

            job2.setOutputKeyClass(NullWritable.class);
            job2.setOutputValueClass(LongWritable.class);
            return job2;
        }

        public static class MyMapper1 extends Mapper<LongWritable, Text, Text, LongWritable> {

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

                // 获取每一行的数据，按照指定分隔符进行拆分
                String[] splits = value.toString().split(" ");

                for (String split : splits) {
                    double random = Math.random();
                    int randomNum = (int) (random * 10);

                    context.write(new Text(randomNum + "_" + split), new LongWritable(1));
                }
            }

            public MyMapper1() {
            }
        }


        public static class MyReducer1 extends Reducer<Text, LongWritable, Text, LongWritable> {

            @Override
            protected void reduce(Text text, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

                int count = 0;

                for (LongWritable value : values) {
                    count += value.get();
                }
                context.write(text, new LongWritable(count));
            }

            public MyReducer1() {
            }
        }

        public static class MyMapper2 extends Mapper<LongWritable, Text, Text, LongWritable> {

            @Override
            protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
                // 获取每一行的数据，按照指定分隔符进行拆分
                String[] splits = value.toString().split(" ");

                for (String split : splits) {
                    String keyValue = split.split("_")[1].split("\t")[0];

                    context.write(new Text(keyValue), new LongWritable(1));
                }
            }

            public MyMapper2() {
            }
        }


        public static class MyReducer2 extends Reducer<Text, LongWritable, Text, LongWritable> {


            @Override
            protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
                int count = 0;

                for (LongWritable value : values) {
                    count += value.get();
                }

                context.write(key, new LongWritable(count));
            }

            public MyReducer2() {
            }
        }
    }
