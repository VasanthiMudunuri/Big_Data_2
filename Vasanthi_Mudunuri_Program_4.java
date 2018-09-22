import java.io.IOException;
import java.util.ArrayList;
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

public class Vasanthi_Mudunuri_Program_4 {
        public static class RatingsMapper extends Mapper<Object,Text,Text,IntWritable>{
                public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
                        String[] record=value.toString().split("::"); //splitting the incoming value and storing in an array of strings
                        int movieid=Integer.parseInt(record[1]); //storing the movieid in a variable movieid
                        int rating=Integer.parseInt(record[2]); //storing the rating in a variable rating
                                try
                                {
                                context.write(new Text(Integer.toString(movieid)),new IntWritable(rating)); //generating output keyvalue pairs
                                }
                                catch (Exception e) {  //catching exceptions if any
                                        System.out.println(e.getMessage());
                                }
                }
        }
        public static class RatingsReducer extends Reducer<Text,IntWritable,Text,FloatWritable>
        {
                public void reduce(Text key,Iterable<IntWritable> values,Context context)
                                throws IOException, InterruptedException{
                        int sum=0; //creating variable sum
                        int count=0; //creating variable count
                        float mean=0; //creating variable mean
                        float squareofdiff=0; //creating variable square of differences
                        ArrayList<Integer> ratings=new ArrayList<Integer>(); //creating ratings list to store all the ratings
                        for(IntWritable value: values) //iterating through incoming values
                        {
                                ratings.add(Integer.parseInt(value.toString())); //adding the ratings to ratings list
                        }
                        for(Integer rating:ratings) //iterating through ratings
                        {
                            sum+=rating; //adding all ratings
                            count++; //counting the ratings
                        }
                    for(Integer ratingmean:ratings) //iterating through ratings
                    {
                        mean=(float)sum/count; //calaculating mean
                        squareofdiff=(float)((ratingmean-mean)*(ratingmean-mean)); //calculating square of differences
                        context.write(key,new FloatWritable(squareofdiff)); //generating output keyvalue pairs
                    }
                }
        }
        public static void main(String[] args) throws Exception{
                Configuration conf=new Configuration(); //setting the configuartion
                Job job=Job.getInstance(conf,"rating SD count"); //setting the job
                job.setJarByClass(Vasanthi_Mudunuri_Program_4.class); //setting the jar by class
                job.setMapperClass(RatingsMapper.class); //setting mapper class
                job.setReducerClass(RatingsReducer.class); //setting reducer class
                job.setMapOutputKeyClass(Text.class); //setting mapper output key class
                job.setMapOutputValueClass(IntWritable.class); //setting mapper output value class
                job.setOutputKeyClass(Text.class); //setting putput key class
                job.setOutputValueClass(FloatWritable.class); //setting output value class
                FileInputFormat.addInputPath(job,new Path(args[0])); //setting input path
                FileOutputFormat.setOutputPath(job,new Path(args[1])); //setting output path
                System.exit(job.waitForCompletion(true)? 0:1); //waiting for the job completion

        }

}

