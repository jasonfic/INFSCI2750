package group4.MiniProject1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HadoopNGramFrequency {
	// Adapted from Apache's MapReduce tutorial: 
	// https://hadoop.apache.org/docs/current/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html
	
	private static int n = 1;

	public static class NGramMapper extends Mapper<Object, Text, Text, IntWritable>{

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
	    	int n = conf.getInt("n", 1);
	    	System.out.print("Mapper value for n: ");
	    	System.out.println(n);
	    	
	        String input = value.toString();
	        input = input.replaceAll("\\s+", "");
	        List<String> joined = new ArrayList<String>();
	        
	        // Split input text with a space as a delimiter every n characters
			for (int i = 0; i < input.length()-(n-1); i++) {
				String index = input.substring(i, i+n);
				joined.add(index);
			}
			
			String line = String.join(" ", joined);
	        
	        StringTokenizer itr = new StringTokenizer(line);
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				context.write(word, one);
			}
		}
	}
	
	public static class NGramReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		// Collect n value from command line input while avoiding out of bounds exceptions
		if (args.length > 2) {
			n = Integer.parseInt(args[2]);
		}
		System.out.print("Text split into chunks of size: ");
		System.out.println(n);
		
		Configuration conf = new Configuration();
		conf.setInt("n", n);
		Job job = Job.getInstance(conf, "ngram frequency counter");
		job.setJarByClass(HadoopNGramFrequency.class);
		job.setMapperClass(NGramMapper.class);
		job.setCombinerClass(NGramReducer.class);
		job.setReducerClass(NGramReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}
