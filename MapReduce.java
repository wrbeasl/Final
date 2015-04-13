import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.joda.time.DateTime;
import org.joda.time.Seconds;


public class MapReduce { 
	

	private static HashMap<String, String[]> matchMap = new HashMap<String, String[]>();
	private static HashMap<String, Float> hpMap = new HashMap<String, Float>();
	static String PID, TS;
	
	public static class Map extends Mapper<LongWritable, Text, Text, Review>{
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			System.out.println(line);
			String[] split = line.split(" ");

				if(matchMap.containsKey(split[0])){
					String ts = matchMap.get(split[0])[1];
					String hp = matchMap.get(split[0])[0];
					
					Review r = new Review(new FloatWritable(Float.parseFloat(hp)),
							   new LongWritable(Long.parseLong(ts)),
							   new LongWritable(Long.parseLong(split[1])));
					
					context.write(new Text(split[0]), r);
				}
			
		}
		
		@Override
		public void setup(Context context) throws IOException{
			Configuration conf = context.getConfiguration();
			String param = conf.get("matchfile");
			InputStream is = new FileInputStream(param);
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			
			String line = null;
			while((line = br.readLine())!=null){
				// Split the line
				String[] split = line.split("::");
				// Make an array
				String[] temp = new String[2];
				// This is the time stamp
				temp[0] = split[1];
				// This is the helpfulness
				temp[1] = split[2];
				matchMap.put(split[0], temp);
			}
			
			is.close();
			isr.close();
			br.close();
		}
	}
		
	public static class Reduce extends Reducer<Text, Review, Text, Text>{

		
		// The key is the product ID and the Float are the helpfulness of reviews
		@Override
		public void reduce(Text key, Iterable<Review> values, Context context) throws IOException, InterruptedException{
			
			String curProduct = key.toString();
			for(Review r : values){
				DateTime release = new DateTime(r.getRD());
				DateTime post = new DateTime(r.getTS());
				Seconds seconds = Seconds.secondsBetween(release, post);
				
				context.write(new Text(curProduct), new Text(seconds.getSeconds()+""));
			}
		}

	
	}
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		conf.set("matchfile", "helpfulness.txt");
		
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "MapReduce");
		job.setJarByClass(Main.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Review.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("release_dates.txt"));
		FileOutputFormat.setOutputPath(job, new Path("output2"));
		
		job.waitForCompletion(true);
	}
	


}