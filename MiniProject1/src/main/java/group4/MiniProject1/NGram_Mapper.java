package group4.MiniProject1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper.Context;

public abstract class NGram_Mapper implements Mapper<Object,Text,Text,IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
    	Configuration conf = context.getConfiguration();
    	int n = conf.getInt("n", 1);
    	
        String input = value.toString();
        input = input.replaceAll("\\s+", "");
        List<String> joined = new ArrayList<String>();
        
		for (int i = 0; i < input.length()-(n-1); i++) {
			String index = input.substring(i, i+n);
			joined.add(index);
		}
		
		String line = String.join(" ", joined);
        
        StringTokenizer  tokenizer = new StringTokenizer(line);
        while (tokenizer.hasMoreTokens()){
            word.set(tokenizer.nextToken());
            context.write(word, one);
        }
    }


}
