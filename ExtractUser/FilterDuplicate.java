import java.io.IOException;
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
import org.apache.hadoop.util.GenericOptionsParser;

public class FilterDuplicate {

  public static class UMapper 
       extends Mapper<Object, Text, Text, Text>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text tweetID = new Text();
    private Text remaining = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      	String [] result = (value.toString()).split("\t");
	String remaining_String = "";

			tweetID.set(result[0]);
			for(int i =2;i<result.length;i++)
				remaining_String = remaining_String + result[i] + "\t";
			remaining.set(remaining_String);
			context.write(tweetID, remaining);
		
		
		
		
	}
  }
  
  public static class UReducer 
       extends Reducer<Text,Text,Text,Text> {
    private Text tweet = new Text();
     public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
		for (Text val : values)
     			tweet = val;
      context.write(key, tweet);
    }
  }

  public static void main(String[] args) throws Exception {
	Configuration conf= new Configuration();
    Job job = new Job(conf, "FilterDuplicate");
    job.setJarByClass(FilterDuplicate.class);
    job.setMapperClass(UMapper.class);
    job.setReducerClass(UReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
	job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job, new Path("/input/"));
    FileOutputFormat.setOutputPath(job, new Path("/output/"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
