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

public class ParseUsers {

  public static class UMapper 
       extends Mapper<Object, Text, Text, IntWritable>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text userid = new Text();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String [] result = (value.toString()).split("\t");
		if(result.length==7)
		{
			//System.out.println();
			//System.out.println(result.length);
			userid.set(result[5]);
			context.write(userid, one);
		}
		
		
		
	}
  }
  
  public static class UReducer 
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
	Configuration conf= new Configuration();
    Job job = new Job(conf, "Parse User");
    job.setJarByClass(ParseUsers.class);
    job.setMapperClass(UMapper.class);
    job.setReducerClass(UReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
	job.setNumReduceTasks(1);
    FileInputFormat.addInputPath(job, new Path("/project/input/"));
    FileOutputFormat.setOutputPath(job, new Path("/project/output/"));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
