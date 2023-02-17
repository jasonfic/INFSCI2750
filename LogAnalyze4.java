package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class LogAnalyze4 {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "LogAnalyze4");
        job.setJarByClass(LogAnalyze4.class);
        job.setMapperClass(LogAnalyze4.accessLogMap.class);
        job.setReducerClass(LogAnalyze4.accessLogReduce.class);

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
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String textValue = value.toString();
            int quoteIndex = textValue.indexOf('\"');
            int spaceIndex1 = textValue.indexOf(" ", quoteIndex);
            int spaceIndex2 = textValue.indexOf(" ", spaceIndex1+1);
            if(spaceIndex1!=-1 && spaceIndex2!=-1){
                String url = textValue.substring(spaceIndex1+1, spaceIndex2);
                context.write(new Text(url), valueOne);
            }
        }
    }

    public static class accessLogReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        static int max = 0;
        Text pathMax = new Text();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable i : values) sum += i.get();
            if(max<sum){
                max = sum;
                pathMax.set(key);
            }
        }
        public void cleanup(Context context) throws IOException, InterruptedException{
            context.write(pathMax, new IntWritable(max) );
        }
    }

}
