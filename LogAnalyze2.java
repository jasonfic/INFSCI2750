package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class LogAnalyze2 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf, "LogAnalyze2");
        job.setJarByClass(LogAnalyze2.class);
        job.setMapperClass(LogAnalyze2.accessLogMap.class);
        job.setReducerClass(LogAnalyze2.accessLogReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
    public static class accessLogMap extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable valueOne = new IntWritable(1);
        private final static String URLtoMatch = "10.153.239.5";
        private final static Text matchPattern = new Text(URLtoMatch);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String textValue = value.toString();
            if(textValue.contains(URLtoMatch)) context.write(matchPattern,valueOne);
        }
    }
    public static class accessLogReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values) sum += i.get();
            context.write(key, new IntWritable(sum));
        }
    }


}
