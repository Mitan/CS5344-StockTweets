package wordcount.project;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

	public static class Mapper1 extends Mapper<Object, Text, Text, Text> {

		private Set<String> inwords = new HashSet<String>();
		private Text date = new Text();

		@Override
		protected void setup(Context context) {
			try {
				// set up the stop words
				Path path = new Path("/project/contains/words.txt");
				FileSystem fs = FileSystem.get(new Configuration());
				BufferedReader br = new BufferedReader(new InputStreamReader(
						fs.open(path)));
				String word = null;
				while ((word = br.readLine()) != null) {
					inwords.add(word);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}

		}

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			if (itr.hasMoreTokens()) {
				// ignore id
				itr.nextToken();
				if (itr.hasMoreTokens()) {
					// get the date
					date.set(itr.nextToken());
					if (itr.hasMoreTokens()) {
						// ignore the time
						itr.nextToken();
					}
					if (date.toString().startsWith("201")) {
						while (itr.hasMoreTokens()) {
							String nToken = itr.nextToken();
							for (String s1 : inwords) {
								if (s1.equalsIgnoreCase(nToken)) {
									context.write(
											new Text(date + " " + nToken),
											new Text(value.toString()));
									break;
								}
							}
						}
					}
				}
			}
		}
	}

	public static class Reducer1 extends Reducer<Text, Text, Text, IntWritable> {
		public void reduce(Text key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (Text s : value) {
				System.out.println("key=" + key + " Tweet=" + s);
				sum++;
			}
			context.write(new Text(key), new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		// args using generic option parser
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		Path input = new Path(otherArgs[0]);
		Path output = new Path(otherArgs[1]);
		Job job1 = new Job(conf, "stage1");
		job1.setJarByClass(WordCount.class);
		job1.setMapperClass(Mapper1.class);
		job1.setReducerClass(Reducer1.class);
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job1, input);
		FileOutputFormat.setOutputPath(job1, output);
		if (!job1.waitForCompletion(true)) {
			System.exit(1);
		}

		System.exit(0);
	}
}
