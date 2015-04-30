import java.io.IOException;
import java.util.StringTokenizer;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.Set;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;

public class NumEdge {
  public static Set<String> edges = new HashSet<String>();
  public static class UMapper 
       extends Mapper<Object, Text, Text, Text>{
    
    private final static IntWritable one = new IntWritable(1);
    private Text tweetID = new Text();
    private Text remaining = new Text();
      
	protected void setup(Context context){
		

	}
    public void map(Object key, Text value, Context context   
                    ) throws IOException, InterruptedException {
      	String [] result = (value.toString()).split("\t");
		String remaining_String = "";
		if (result.length >= 7)
		{
			String tweet_time = result[result.length-6];
			String[] parsed_value = tweet_time.split(" ");
			String date = parsed_value[0];
			String tweet_user_id = result[result.length-2];
		//~ if(!spammers.contains(tweet_user_id))

			context.write(new Text(date), new Text(tweet_user_id));
		
		}
	}
  }
  
  public static class UReducer 
       extends Reducer<Text,Text,Text,Text> {
     public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
		ArrayList<String> node_array =  new ArrayList<String>();			   
		for (Text val : values)
		{
			node_array.add(val.toString());
		}
		int num_edges = 0;
		for (int i = 0; i < node_array.size(); i++) {
			for (int j = i; j < node_array.size(); j++){
				String edge1 = node_array.get(i) + "@" + node_array.get(j);
				String edge2 = node_array.get(j) + "@" + node_array.get(i);
				if (edges.contains(edge1) || edges.contains(edge2))
					num_edges = num_edges+1;
			}			
		}
		context.write(key, new Text(String.valueOf(num_edges)));
    }
  }
  
  public static void parse_graph() throws Exception
  {
	  	try {
			Path path = new Path("/graph/graph_out.txt");
			FileSystem fs= FileSystem.get(new Configuration());
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
			String line = null;
			while ((line= br.readLine())!= null) {
				StringTokenizer itr = new StringTokenizer(line);
				if (itr.countTokens() > 2)
				{
					String source = itr.nextToken();
					String num_edge = itr.nextToken();
					while (itr.hasMoreTokens()) {
						edges.add(source + "@" + itr.nextToken());
					}
				}
			
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		}
  }

  public static void main(String[] args) throws Exception {
	parse_graph();
	Configuration conf= new Configuration();
    Job job = new Job(conf, "NumEdge");
    job.setJarByClass(NumEdge.class);
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
