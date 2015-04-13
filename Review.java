import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class Review implements Writable{
		LongWritable timestamp;
		FloatWritable helpfulness;
		LongWritable release_date;
		
		public Review(){}
		
		public Review(FloatWritable help, LongWritable ts, LongWritable rd){
			timestamp = ts;
			helpfulness = help;
			release_date = rd;
		}
		
		public LongWritable getTS(){ return timestamp; }
		public LongWritable getRD(){ return release_date; }
		public FloatWritable getHP(){ return helpfulness; }
		public void setTS(LongWritable ts){ timestamp = ts; }
		public void setHP(FloatWritable hp){ helpfulness = hp; }
		public void setRD(LongWritable rd){ release_date = rd; }

		@Override
		public void readFields(DataInput arg0) throws IOException {
			timestamp.readFields(arg0);
			helpfulness.readFields(arg0);
			release_date.readFields(arg0);
		}

		@Override
		public void write(DataOutput arg0) throws IOException {
			timestamp.write(arg0);
			helpfulness.write(arg0);
			release_date.write(arg0);
		}
		
		public static Review read(DataInput in) throws IOException{
			Review r = new Review();
			r.readFields(in);
			return r;
		}
		
			
}
