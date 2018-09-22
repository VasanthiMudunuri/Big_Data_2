import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;

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

public class Vasanthi_Mudunuri_Program_2 {
        public static class MovieMapper extends Mapper<Object,Text,IntWritable,Text>{
                public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
                        String[] record=value.toString().split("::"); //splitting the incoming value and storing in an array of strings
                        String moviename=record[1];  //storing moviename in variable moviename
                        String moviegenre=record[2]; //storing moviegenre in variable moviegenre
                                try
                                {
                                context.write(new IntWritable(1),new Text(moviename+"::"+moviegenre)); //generating output keyvalue pair
                                }
                                catch (Exception e) { //catching exceptions if any
                                        System.out.println(e.getMessage());
                                }
                }
        }
        public static class MovieReducer extends Reducer<IntWritable,Text,Text,FloatWritable>
        {
                public void reduce(IntWritable key ,Iterable<Text> values,Context context)
                                throws IOException, InterruptedException{
           String[] genrelist=new String[18]; //craeting a list of genres
                        genrelist[0]="Action";
                        genrelist[1]="Adventure";
                        genrelist[2]="Animation";
                        genrelist[3]="Children's";
                        genrelist[4]="Comedy";
                        genrelist[5]="Crime";
                        genrelist[6]="Documentary";
                        genrelist[7]="Drama";
                        genrelist[8]="Fantasy";
                        genrelist[9]="Film-Noir";
                        genrelist[10]="Horror";
                        genrelist[11]="Musical";
                        genrelist[12]="Mystery";
                        genrelist[13]="Romance";
                        genrelist[14]="Sci-Fi";
                        genrelist[15]="Thriller";
                        genrelist[16]="War";
                        genrelist[17]="Western";
                        Iterator<Text> incominggenre=values.iterator(); //to iterate through incoming values
                        ArrayList<String> moviegenre=new ArrayList<String>(); //list of moviegenres 
                        ArrayList<String> movienames=new ArrayList<String>(); //list of movienames
                        ArrayList<String> genrenames=new ArrayList<String>(); //list of genrenames
                        HashMap<String,Float> genreoccurences=new HashMap<String,Float>(); //hashmap genreoccurences to store genres and its corresponding average of number of movies
                        int count=0;
                        int totalmovies=0;
                        while(incominggenre.hasNext()) //iterating through incoming values
                        {
                                String[] value=incominggenre.next().toString().split("::"); //splitting the values and storing in an array of strings
                                if(value[0]!=null) //condition to check if value is null
                                {
                                String moviename=value[0]; //storing moviename in variable moviename
                                movienames.add(moviename); //adding movienames to movienames list
                                String getgenre=value[1]; //storing genre in variable getgenre
                                moviegenre.add(getgenre); //adding genres to moviegenre list
                                }
                        }
                        for(String name:movienames) //iterating through movienames
                        {
                                if(name!=null)
                                  {
                                totalmovies++; //counting total movies
                                }
                        }
                        for(String i:moviegenre) //iterating through moviegenre
                        {
                        String[] genreseperation=i.split("\\|"); //splitting moviegenre and storing in an array of strings
                        for(String s:genreseperation) //iterating through the string array
                        {
                                genrenames.add(s); //adding genres to list genrenames
                        }
                        }
                        for(int i=0;i<genrelist.length;i++) //condition to iterate through genrelist
                        {
                                for(String j: genrenames) //iterating through strings in genrenames
                                      {
                                          if(j.equals(genrelist[i])) //condition to match genrenames with the genrelist elements
                                          {
                                                  count++; //counting the occurences
                                          }
                                      }
                                      genreoccurences.put(genrelist[i],(float)count/totalmovies); //adding genre and its corresponding average of number of movies 
                                          count=0; //resetting count
                        }
                        TreeMap<String,Float> sortedoccupations = new TreeMap<String,Float>(Collections.reverseOrder()); //Treemap sortedoccupations to sort the key in descending order
                        for (Entry<String,Float> entry : genreoccurences.entrySet()) //iterating through genreoccurenecs
                        {
                            sortedoccupations.put(entry.getKey(),entry.getValue()); //adding genreoccurences to sortedoccupations
                        }
                        for(Map.Entry<String,Float> entry: sortedoccupations.entrySet()) //iterating through sortedoccupations
                        {
                                context.write(new Text(entry.getKey()),new FloatWritable(entry.getValue())); //generating output keyvalue pairs
                        }
                }
        }
        public static void main(String[] args) throws Exception{
                Configuration conf=new Configuration(); //setting the configuartion
                Job job=Job.getInstance(conf,"movie count"); //setting the job 
                job.setJarByClass(Vasanthi_Mudunuri_Program_2.class); //setting jar by class
                job.setMapperClass(MovieMapper.class); //setting mapper class
                job.setReducerClass(MovieReducer.class); //setting reducer class
                job.setMapOutputKeyClass(IntWritable.class); //setting mapper output key class
                job.setMapOutputValueClass(Text.class); //setting mapper output value class
                job.setOutputKeyClass(Text.class); //setting output key class
                job.setOutputValueClass(FloatWritable.class); //setting output value class
                FileInputFormat.addInputPath(job,new Path(args[0])); //setting input path
                FileOutputFormat.setOutputPath(job,new Path(args[1])); //setting output path
                System.exit(job.waitForCompletion(true)? 0:1); //waiting for the job completion
        }

}
