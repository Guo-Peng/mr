package com.guopeng.bigdata;

import com.guopeng.bigdata.secondary.SecondaryGroup;
import com.guopeng.bigdata.secondary.SecondaryKey;
import com.guopeng.bigdata.secondary.SecondaryPartitioner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.guopeng.bigdata.utils.Utils.deleteDir;

/**
 * Created by guopeng on 17-4-19.
 */
public class SecondarySort {
    public static class TokenizerMapper extends Mapper<Object, Text, SecondaryKey, IntWritable> {
        SecondaryKey result = new SecondaryKey();
        Text word = new Text();
        IntWritable one = new IntWritable();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();

            String reg = "(.*?)(\\d+)";
            Pattern pattern = Pattern.compile(reg);
            Matcher matcher = pattern.matcher(line);
            if (matcher.find()){
                word.set(matcher.group(1));
                one.set(Integer.valueOf(matcher.group(2)));
                result.setFirstKey(word);
                result.setSecondKey(one);
                context.write(result, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<SecondaryKey, IntWritable, Text, IntWritable> {

        public void reduce(SecondaryKey key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            for (IntWritable val : values) {
                context.write(key.getFirstKey(), val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: track <in> <out1> <out2>");
            System.exit(2);
        }
        deleteDir(otherArgs[1]);

        // 第一个job的配置
        Job job = new Job(conf, "word count");

        job.setJarByClass(SecondarySort.class);

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);

        job.setMapOutputKeyClass(SecondaryKey.class);// map阶段的输出的key
        job.setMapOutputValueClass(IntWritable.class);// map阶段的输出的value

        job.setOutputKeyClass(Text.class);// reduce阶段的输出的key
        job.setOutputValueClass(IntWritable.class);// reduce阶段的输出的value

        job.setPartitionerClass(SecondaryPartitioner.class); //设置自定义分区策略
        job.setGroupingComparatorClass(SecondaryGroup.class); //设置自定义分组策略

        job.setNumReduceTasks(10);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
