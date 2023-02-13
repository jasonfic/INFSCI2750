package group4.MiniProject1;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper.Context;

public abstract class NGram_Reducer  extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable> {
	
	public void reduce(Text key, Iterator<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum=0;
		
        while (values.hasNext()) {
            sum+=values.next().get();
        }
        
        context.write(key, new IntWritable(sum));
	}
}
