import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Vasanthi_Mudunuri_Program_3 {

        public static class MovieMapper extends Mapper<Object,Text,Text,IntWritable>{
                public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
                        Configuration conf=context.getConfiguration(); //getting the configuration
                        int movieID=Integer.parseInt(conf.get("movieID")); //getting movieID from configuration and storing in variable movieID
                        String[] record=value.toString().split("::"); //splitting the incoming value and storing in an array of strings
                        int movieid=Integer.parseInt(record[1]); //storing movieid in variable movieid
                        int rating=Integer.parseInt(record[2]); //storing rating in variable rating
                        if(movieid==movieID) //matching incoming movieID with given movieID
                        {
                                try
                                {
                                context.write(new Text(Integer.toString(movieid)),new IntWritable(rating)); //generating output keyvalue pairs
                                }
                                catch (Exception e) { //catching exceptions if any
                                        System.out.println(e.getMessage());
                                }

                        }
                }
        }
        public static class MovieReducer extends Reducer<Text,IntWritable,Text,FloatWritable>
        {
                public void reduce(Text key,Iterable<IntWritable> values,Context context)
                                throws IOException, InterruptedException{
                        int sum=0; //creating variable sum
                        int count=0; //creating variable count
                        for(IntWritable value: values) //iterating through incoming values
                        {
                                sum+=value.get(); //adding all ratings
                                count++; //counting the ratings
                        }
                        context.write(new Text(key),new FloatWritable((float)sum/count)); //generating output keyvalue pairs
                        }
                }
        public static void main(String[] args) throws Exception{
                Configuration conf=new Configuration(); //setting the configuration
                conf.set("movieID",args[2]); //setting the movieID with given argument
                Job job=Job.getInstance(conf,"rating count"); //setting the job
                job.setJarByClass(Vasanthi_Mudunuri_Program_3.class); //setting jar by class
                job.setMapperClass(MovieMapper.class); //setting mapper class
                job.setReducerClass(MovieReducer.class); //setting reducer class 
                job.setMapOutputKeyClass(Text.class); //setting mapper output key class
                job.setMapOutputValueClass(IntWritable.class); //setting mapper output value class
                job.setOutputKeyClass(Text.class); //setting output key class
                job.setOutputValueClass(FloatWritable.class); //setting output value class
                FileInputFormat.addInputPath(job,new Path(args[0])); //setting input path
                FileOutputFormat.setOutputPath(job,new Path(args[1])); //setting output path
                System.exit(job.waitForCompletion(true)? 0:1); //waiting for the job completion
        }


}
