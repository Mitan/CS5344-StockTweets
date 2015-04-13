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
	public static final float beta = 0.85f;
	public static float teleport = 0.15f;
	//to store the current page_rank
	public static ConcurrentHashMap<String,Float>  page_rank = new ConcurrentHashMap<String,Float>();
	public static ConcurrentHashMap<String,Float>  error_page_rank =  new ConcurrentHashMap<String,Float>(); 
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
					error_page_rank.put(id,new Float(0.0f));
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
		  // need 1 key/value pair for each node, because there are nodes that have no outlinks and inlinks
			context.write(new Text(node_id), new FloatWritable(0.0f));
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
		error_page_rank.put(key.toString(),final_rank - page_rank.get(key.toString()).floatValue());
		page_rank.put(key.toString(),final_rank);
	
		//result.set(sum);
		
		//no need to write anything// all update in hashmap
		if (stop == true)
			context.write(key, new FloatWritable(final_rank));
  }
}



 public static class SortMapper 
       extends Mapper<Object, Text, Text, FloatWritable>{
    
     public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
     StringTokenizer itr = new StringTokenizer(value.toString());
      //get the name of file containing this key/value pair 
      if(itr.countTokens() ==2)
      {
		
			context.write(new Text(itr.nextToken()), new FloatWritable(Float.parseFloat(itr.nextToken())));
		}
	}
}
  
  public static class SortReducer 
       extends Reducer<Text,FloatWritable,Text,FloatWritable> {
	private TreeMap <Float, ArrayList<String>> tmap = null;
	
	public void setup(Context context) throws IOException, InterruptedException { 
		tmap = new TreeMap<Float, ArrayList<String>>();
	}
    public void reduce(Text key, Iterable<FloatWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
       for (FloatWritable val : values) {

		  if(tmap.containsKey(val.get()))
			{
				ArrayList<String> user_list = tmap.get(val.get());
				user_list.add(key.toString());
			}
			// otherwise, insert the file into a new ArrayList (file_list)
			else
			{
				 ArrayList<String> user_list = new ArrayList<String>();
				 user_list.add(key.toString());
				 tmap.put(val.get(),user_list);
			}
			
	   }
   }
    
    public void cleanup(Context context) throws IOException,InterruptedException {
		//copy treemap into navigablemap in a reverse order of keys
		NavigableSet<Float> reverseKeyList = tmap.descendingKeySet();	
		for(Float key : reverseKeyList) {
				ArrayList<String> value = tmap.get(key);
				// note: need to extract the files from the each ArrayList.
			//	context.write(new Text(String.valueOf(key)), new FloatWritable(value.size()));
				for (int i = 0; i < value.size(); i++) {
					 context.write(new Text(value.get(i)), new FloatWritable(key));
				}
		}	
	}
  }





public static void delete_output(String output_path)throws Exception
{
	FileSystem fs = FileSystem.get(new Configuration());
	fs.delete(new Path(output_path), true); // delete file, true for recursive 
}

public static float calculate_error()throws Exception
{
	float sum_error = 0.0f;
	for (String key : error_page_rank.keySet()) {
		 if( Math.abs(error_page_rank.get(key)) > sum_error)
			sum_error = Math.abs(error_page_rank.get(key));
	}
	//sum_error = sum_error/matrix_size;
	return sum_error;
	
}

//Main function
  public static void main(String[] args) throws Exception {
	  
	initiallize(); 
	matrix_size =  page_rank.size();
	teleport = (1.0f - beta);
	int max_runs = 30;
	
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
		
		System.out.println ("----------average change in previous stage--------");
		System.out.println(calculate_error());
	}

		Configuration conf_stage2 = new Configuration();
		Job stage2 = Job.getInstance(conf_stage2, "Stage2");
		stage2.setJarByClass(Pagerank.class);
		stage2.setMapperClass(SortMapper.class);
		stage2.setReducerClass(SortReducer.class);
		stage2.setOutputKeyClass(Text.class);
		stage2.setOutputValueClass(FloatWritable.class);

		FileInputFormat.addInputPath(stage2, new Path("/output/"));
		FileOutputFormat.setOutputPath(stage2, new Path("/sorted_output/"));
		stage2.waitForCompletion(true);	
     

  }
}
