import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Wordlengthaverage {
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private IntWritable len = new IntWritable();
        private Text first = new Text();
	        
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	        String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line);
	        while(tokenizer.hasMoreTokens()) {   
	        	String word=tokenizer.nextToken();
	        	len.set(word.length());
	        	first.set(String.valueOf(word.charAt(0)));
	        	context.write(first, len);
	        	System.out.println(first);
	        	System.out.println(len);
	        }
	    }
	 } 
	 
	
	 public static class Reduce extends Reducer<Text, IntWritable, Text, DoubleWritable> {
		 
		 private DoubleWritable avg=new DoubleWritable();
		
	    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
	      throws IOException, InterruptedException {
	        int sum=0,count=0;
	        //double average=0.0;
	        while (values.iterator().hasNext()) {
	        	IntWritable val=values.iterator().next();
	        	sum+=val.get();
	        	count=count+1;
	        }
	        double average = (double)sum/count;
	        avg.set(average);
	        context.write(key, avg);
	        System.out.println(key);
	        System.out.println(average);
	    }
	 }
	
	public static void main(String[] args) throws Exception
	{
		 Configuration conf = new Configuration();
	        
		    Job job=Job.getInstance(conf,"word length average");
		    job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(IntWritable.class);
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(DoubleWritable.class);
		        
		    job.setMapperClass(Map.class);
		    job.setReducerClass(Reduce.class);
		        
		    job.setInputFormatClass(TextInputFormat.class);
		    job.setOutputFormatClass(TextOutputFormat.class);
		        
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		        
		    job.waitForCompletion(true);
		    System.out.println("Completed");
	}
	
}
