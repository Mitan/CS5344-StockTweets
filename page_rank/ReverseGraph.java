import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.Math;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.NavigableMap;
import java.util.NavigableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

//Reconstruct the graph from:
//node_id : list of edge-in to node_id : list of edge-out
//and vice versa
public class ReverseGraph {

	public static class ReverseMapper extends Mapper<Object, Text, Text, Text>{
	private Text result_key = new Text();
	private Text result_value = new Text();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		//read each line and convert into key = its followers, value = node_id;
		//next group by key, so we created a matrix of out_links
      StringTokenizer itr = new StringTokenizer(value.toString());
      //get the name of file containing this key/value pair 
      if(itr.countTokens() >=1)
      {
		  result_value.set(itr.nextToken());
		  
		  if (itr.nextToken().equals('0') == false)
		  {
			  while (itr.hasMoreTokens()) {
				result_key.set(itr.nextToken());
				context.write(result_key, result_value);
			  }
		  }
		  
	  }
    }
  }
  
//Reducer: created a matrix of out_links
public static class ReverseReducer extends Reducer<Text,Text,Text,Text> {
	private Text result_value = new Text();
   
    public void reduce(Text key, Iterable<Text> values, Context context)
    throws IOException, InterruptedException {
		String result = "";
		int count = 0;
		for (Text val : values) {
			result += " " + val.toString();
			count++;
		}
		result_value.set(" " + String.valueOf(count) + result);
		context.write(key, result_value);
  }
}

//Main function
  public static void main(String[] args) throws Exception {
// Stage 1
	Configuration conf_stage1 = new Configuration();
    Job stage1 = Job.getInstance(conf_stage1, "Stage1");
    stage1.setJarByClass(ReverseGraph.class);
    stage1.setMapperClass(ReverseMapper.class);
    stage1.setReducerClass(ReverseReducer.class);
    stage1.setOutputKeyClass(Text.class);
    stage1.setOutputValueClass(Text.class);

	FileInputFormat.addInputPath(stage1, new Path("/input"));
    FileOutputFormat.setOutputPath(stage1, new Path("/output"));
	stage1.waitForCompletion(true); 
     
     
	//System.exit(stage1.waitForCompletion(true) ? 0 : 1);

  }
}
