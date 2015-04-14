import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
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
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;


public class MapReduce { 
	

	private static HashMap<String, String[]> matchMap = new HashMap<String, String[]>();
	
    public static class TextArrayWritable extends ArrayWritable {
        public TextArrayWritable() {
            super(Text.class);
        }

        public TextArrayWritable(String[] strings) {
            super(Text.class);
            Text[] texts = new Text[strings.length];
            for (int i = 0; i < strings.length; i++) {
                texts[i] = new Text(strings[i]);
            }
            set(texts);
        }
    }

	
	public static class Map extends Mapper<LongWritable, Text, Text, ArrayWritable>{
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String line = value.toString();
			String PID = line.substring(0, line.indexOf(" "));
			String RD  = line.substring(line.indexOf(" ")+2, line.length());
				if(matchMap.containsKey(PID)){
					String[] ts = matchMap.get(PID);
					String[] hp = matchMap.get(PID)[1].split("/");
					float l = Float.parseFloat(hp[0]);
					float r = Float.parseFloat(hp[1]);
					float t = l / r;
					
					
					
					String[] txt2 = new String[3];
					txt2[0] = RD;
					txt2[1] = ts[0];
					txt2[2] = (t + "");
				
			
					TextArrayWritable txt = new TextArrayWritable(txt2);
					
						context.write(new Text(PID),txt);
			
				} else { return; }
			
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
		
	public static class Reduce extends Reducer<Text, TextArrayWritable, Text, Text>{

		
		private HashMap<String, String> time = new HashMap<String, String>();
		
		// The key is the product ID and the TextArrayWritable's are the post date, release date, and helpfulness.
		@Override
		public void reduce(Text key, Iterable< TextArrayWritable > values, Context context) throws IOException, InterruptedException{
			

			for(TextArrayWritable t : values){
				String[] txt = new String[3];
				String TS, HP, RD;
				for(int i = 0; i < t.get().length; ++i){

					txt[i] = t.get()[i].toString();
					if(txt[i]==null) txt[i] = 0+"";
					System.out.println(i + " " + txt[i]);
				}
				
				DateTime post = new DateTime(Long.parseLong(txt[1]));
			
			
				String[] MDY = txt[0].split(" ");
				MDY[1] = MDY[1].substring(0, MDY[1].indexOf(","));
				
				int month = 0;
				if(MDY[0].compareTo("January") == 0) month = 1;
				if(MDY[0].compareTo("February") == 0) month = 2;
				if(MDY[0].compareTo("March") == 0) month = 3;
				if(MDY[0].compareTo("April") == 0) month = 4;
				if(MDY[0].compareTo("May") == 0) month = 5;
				if(MDY[0].compareTo("June") == 0) month = 6;
				if(MDY[0].compareTo("July") == 0) month = 7;
				if(MDY[0].compareTo("August") == 0) month = 8;
				if(MDY[0].compareTo("September") == 0) month = 9;
						if(MDY[0].compareTo("October") == 0) month = 10;
				if(MDY[0].compareTo("November") == 0) month = 11;
				if(MDY[0].compareTo("December") == 0) month = 12;
				
				DateTime rd = new DateTime(Integer.parseInt(MDY[2]), month, Integer.parseInt(MDY[1]), 0, 0);
				

				Seconds seconds = Seconds.secondsBetween(rd, post);
				
				String[] output = new String[3];
				output[0] = key.toString();
				output[1] = seconds.toString();
				output[2] = txt[2];
				
				//String t_out = output[1].substring(3, output[1].length()) + " " + output[2];
				context.write(new Text(output[1].substring(3, output[1].length())), new Text(output[2]));

			}
			
		}

	
	}
	public static void main(String[] args) throws Exception{
		
		Configuration conf = new Configuration();
		conf.set("matchfile", "helpfulness.txt");
		
		Job job = Job.getInstance(conf);
		job.setJarByClass(MapReduce.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TextArrayWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("release_dates.txt"));
		FileOutputFormat.setOutputPath(job, new Path("output2"));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
}