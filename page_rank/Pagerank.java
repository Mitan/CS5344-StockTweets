import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.lang.Math;
import java.util.StringTokenizer;
import java.util.HashSet;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Set;
import java.util.Map;
import java.util.TreeMap;
import java.util.ArrayList;
import java.util.NavigableMap;
import java.util.NavigableSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable; 
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

public class Pagerank {
	public static final float beta = 0.8f;
	public static float teleport = 0.2f;
	//to store the current page_rank
	public static ConcurrentHashMap<String,Float>  page_rank = new ConcurrentHashMap<String,Float>();
	public static int matrix_size = 0;
	public static boolean stop = false;
  public static void initiallize()
  {
		try {
			Path path = new Path("/input/initial_pagerank.txt");
			FileSystem fs= FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line = null;
			while ((line= br.readLine())!= null) {
				StringTokenizer itr = new StringTokenizer(line);
				if(itr.countTokens() == 2)
				{
					String id = itr.nextToken();
					float current_rank = Float.parseFloat(itr.nextToken());
					
					page_rank.put(id,new Float(current_rank));
				}
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
  }
  
  
  public static class PagerankMapper extends Mapper<Object, Text, Text, FloatWritable>{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		//read each line and convert into node_id = key / new_rank = value;
      StringTokenizer itr = new StringTokenizer(value.toString());
      //get the name of file containing this key/value pair 
      if(itr.countTokens() >=2)
      {
		  String node_id = itr.nextToken();
		//  result_key.set(node_id);
		  int num_out_links = Integer.parseInt(itr.nextToken());
		  if (num_out_links!= 0)
		  {
			  float current_pagerank = page_rank.get(node_id).floatValue();
			  float distribute_rank = current_pagerank/num_out_links;
			  while (itr.hasMoreTokens()) {
				String out_node = itr.nextToken();
				context.write(new Text(out_node), new FloatWritable(distribute_rank));
			  }
		  }
	  }
    }
  }
  
public static class PagerankReducer extends Reducer<Text,FloatWritable,Text,FloatWritable> {
    private FloatWritable result = new FloatWritable();
    public void reduce(Text key, Iterable<FloatWritable> values, Context context)
    throws IOException, InterruptedException {
		//sum up 
		float sum = 0.0f;
		for (FloatWritable val : values) {
			sum += val.get();
		}
		float final_rank = beta*sum + teleport;
		page_rank.put(key.toString(),final_rank);
		
		//result.set(sum);
		
		//no need to write anything// all update in hashmap
		if (stop == true)
			context.write(key, new FloatWritable(final_rank));
  }
}
public static void delete_output(String output_path)throws Exception
{
	FileSystem fs = FileSystem.get(new Configuration());
	fs.delete(new Path(output_path), true); // delete file, true for recursive 
}

//Main function
  public static void main(String[] args) throws Exception {
	  
	initiallize(); 
	matrix_size =  page_rank.size();
	teleport = (1.0f - beta)/matrix_size;
	int max_runs = 20;
	
	for (int i=0; i<=max_runs ;i++)
	{
		delete_output("/output");
		if (i == max_runs)
			stop = true;
		System.out.println("run ith times : i = " + i);
		Configuration conf_stage1 = new Configuration();
		Job stage1 = Job.getInstance(conf_stage1, "Stage1");
		stage1.setJarByClass(Pagerank.class);
		stage1.setMapperClass(PagerankMapper.class);
		stage1.setReducerClass(PagerankReducer.class);
		stage1.setOutputKeyClass(Text.class);
		stage1.setOutputValueClass(FloatWritable.class);

		FileInputFormat.addInputPath(stage1, new Path("/input/graph_out.txt"));
		FileOutputFormat.setOutputPath(stage1, new Path("/output"));
		stage1.waitForCompletion(true); 
	}
     

  }
}
