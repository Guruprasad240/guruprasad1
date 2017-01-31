import java.io.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


public class Stockechangeavg
{
  public static class MapClass
       extends Mapper<LongWritable, Text, Text, LongWritable>
  {



    public void map(LongWritable key, Text value, Context context)
    {
    	try{
    	String[] str = value.toString().split(",");
    	long vol = Long.parseLong(str[7]);
    	context.write(new Text(str[1]),new LongWritable(vol));
    	}
    	catch(Exception e)
    	{
    		System.out.println(e.getMessage());
    	}
    }
  }
    public static class ReduceClass extends Reducer<Text,LongWritable,Text,FloatWritable>
    {
    	//private LongWritable result = new LongWritable();
    	public void reduce(Text key,Iterable<LongWritable> values,Context context) throws IOException, InterruptedException
    	{
    		long sum = 0;
    		float avg = 0;
    		long count = 0;
    		for (LongWritable val : values) 
    		{
    			sum += val.get();
    			count++;
    		}
    		avg = sum/count;
      //result.set(avg);
      context.write(key,new FloatWritable(avg));
    	}
    }

  public static void main(String[] args) throws Exception 
  
  {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf,"volume count");
    job.setJarByClass(Stockechangeavg.class);
    job.setMapperClass(MapClass.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(ReduceClass.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}