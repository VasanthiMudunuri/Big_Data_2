import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Vasanthi_Mudunuri_Program_5 {

	public static class RatingsMapper2 extends Mapper<Object,Text,Text,FloatWritable>{
		public void map(Object key,Text values,Context context) throws IOException, InterruptedException {
			String[] record=values.toString().split("	"); //splitting the incoming value and storing in an array of strings
			int movieid=Integer.parseInt(record[0]); //storing the movieid in variable movieid
			float squareofdiff=Float.parseFloat(record[1]);  //storing the square of differeneces in a variable squareofdiff
			try
			{
				context.write(new Text(Integer.toString(movieid)),new FloatWritable(squareofdiff)); //setting output keyvalue pairs
			}
			catch (Exception e) { //catching exceptions if any
				System.out.println(e.getMessage());
			}
		}
	}
	public static class RatingsReducer2 extends Reducer<Text,FloatWritable,Text,FloatWritable>
	{
		public void reduce(Text key,Iterable<FloatWritable> values,Context context) 
				throws IOException, InterruptedException{
			ArrayList<Float> ratingsdiffsquare=new ArrayList<Float>(); //creating ratingsdiffsquare list
			float sum=0; //creating variable sum
			int count=0; //creating variable count
			for(FloatWritable value:values) //iterating through incoming values
			{
				ratingsdiffsquare.add(Float.parseFloat(value.toString())); //adding them to the list
			}
			for(float value:ratingsdiffsquare) //iterating through the list
			{
				sum+=value; //adding all the values
				count++; //counting the values
			}
			Float averageofsumofdiff=(float)sum/count;     //calculating the average 
			Float standarddeviation=(float) Math.sqrt(averageofsumofdiff); //calculating the standard deviation
			context.write(key,new FloatWritable(standarddeviation));	//generating the output keyvalue pairs
		}
	}
	public static void main(String[] args) throws Exception
	{
		Configuration conf=new Configuration(); //setting the configuration
		Job job1=Job.getInstance(conf,"rating SD count1"); //setting the job
		job1.setJarByClass(Vasanthi_Mudunuri_Program_5.class); //setting the jar by class
		job1.setMapperClass(RatingsMapper2.class); //setting the mapper class
		job1.setReducerClass(RatingsReducer2.class); //setting the reducer class
		job1.setMapOutputKeyClass(Text.class); //setting mapper output key class
		job1.setMapOutputValueClass(FloatWritable.class); //setting mapper output value class
		job1.setOutputKeyClass(Text.class); //setting output key class
		job1.setOutputValueClass(FloatWritable.class); //setting output value class
		FileInputFormat.addInputPath(job1,new Path(args[0])); //setting input path 
		FileOutputFormat.setOutputPath(job1,new Path(args[1])); //setting output path
		System.exit(job1.waitForCompletion(true)? 0:1); //waiting for the job completion
	}
}
