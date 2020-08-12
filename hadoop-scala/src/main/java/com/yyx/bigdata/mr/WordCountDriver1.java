package com.yyx.bigdata.mr;

import com.yyx.bigdata.util.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
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
public class WordCountDriver1 {

    public static void main(String[] args) throws Exception {
        String input = "yyxnb.txt";
        String output = "out";

        // 1 获取Job
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);


        FileUtils.deleteOutput(configuration, output);

        // 2 设置主类
        job.setJarByClass(WordCountDriver1.class);

        // 3 设置Mapper和Reducer
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 4 设置Mapper阶段输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 5 设置Reduce阶段输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        // 7 提交Job
        boolean result = job.waitForCompletion(true);
        System.exit(result? 0:1 );
    }





    /**
     * KEYIN 输入数据KEY的数据类型
     * VALUEIN 输入数据VALUE的数据类型
     *
     * KEYOUT  输出数据KEY的数据类型
     * VALUEOUT 输出数据VALUE的数据类型
     **/
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

        IntWritable ONE = new IntWritable(1);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            System.out.println("-----------setup----------");
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println("-----------cleanup----------");
        }

        /**
         *
         * @param key   每一行数据的偏移量
         * @param value  每行数据的内容
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            // 获取每一行的数据，按照指定分隔符进行拆分
            String[] splits = value.toString().split(" ");

            // 输出
            for(String word : splits) {
                context.write(new Text(word), ONE);
            }

        }

        public WordCountMapper() {
        }
    }


    /**
     * KEYIN 输入数据KEY的数据类型
     * VALUEIN 输入数据VALUE的数据类型
     *
     * KEYOUT  输出数据KEY的数据类型
     * VALUEOUT 输出数据VALUE的数据类型
     **/
    public static class WordCountReducer extends Reducer<Text, IntWritable,Text, IntWritable> {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            int count = 0 ;

            for(IntWritable value : values) {
                count += value.get();
            }

            context.write(key, new IntWritable(count));

        }

        public WordCountReducer() {
        }
    }

    public WordCountDriver1() {
    }
}
