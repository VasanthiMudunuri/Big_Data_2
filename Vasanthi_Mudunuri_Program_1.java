import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Vasanthi_Mudunuri_Program_1 {

        public static class OccupationMapper extends Mapper<Object,Text,Text,IntWritable>{
                public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
                        String[] record=value.toString().split("::"); //splitting the incoming value and storing in an array of strings
                        String gender=record[1];   //storing gender in variable gender
                        String age=record[2];      //storing age in variable age
                        String occupation=record[3]; //storing occupationID in variable occupation 
                        int getoccupation=0;     //creating variable getoccupation   
                        if(gender.equals("M"))   //condition to check if gender is male
                        {
                                Integer getage=Integer.parseInt(age);   //changing string age to an integer and storing in variable getage
                                try{
                                        if(getage==25 || getage==35) //condition to check if age is between 25 and 45
                                        {
                                                getoccupation=Integer.parseInt(occupation); //changing string occupationID to an integer and storing in variable getoccupation
                                                context.write(new Text(gender),new IntWritable(getoccupation)); //generating output keyvalue pair
                                        }
                                }
                                catch (Exception e) {  //catching exceptions if any
                                        System.out.println(e.getMessage());
                                }

                        }
                }
        }
        public static class OccupationReducer extends Reducer<Text,IntWritable,Text,IntWritable>
        {
                public void reduce(Text key,Iterable<IntWritable> values,Context context)
                                throws IOException, InterruptedException{
                        HashMap<Integer, String> occupationmap=new HashMap<>(); //creating hashmap occupationmap to store occupationID with corresponding name
                        occupationmap.put(0,"\"other\" or not specified");
                        occupationmap.put(1,"academic/educator");
                        occupationmap.put(2,"artist");
                        occupationmap.put(3,"clerical/admin");
                        occupationmap.put(4,"college/grad student");
                        occupationmap.put(5,"customer service");
                        occupationmap.put(6,"doctor/health care");
                        occupationmap.put(7,"executive/managerial");
                        occupationmap.put(8,"farmer");
                        occupationmap.put(9,"homemaker");
                        occupationmap.put(10,"K-12 student");
                        occupationmap.put(11,"lawyer");
                        occupationmap.put(12,"programmer");
                        occupationmap.put(13,"retired");
                        occupationmap.put(14,"sales/marketing");
                        occupationmap.put(15,"scientist");
                        occupationmap.put(16,"self-employed");
                        occupationmap.put(17,"technician/engineer");
                        occupationmap.put(18,"tradesman/craftsman");
                        occupationmap.put(19,"unemployed");
                        occupationmap.put(20,"writer");
                        HashMap<String,Integer> occupationoccurences=new HashMap<String,Integer>(); //creating hashmap occupationoccurences to store occupationID with count of occurences
                        List<Integer> occupationid=new ArrayList<Integer>();  //creating list occupationid to store incoming values
                        for(IntWritable value:values)
                        {
                                occupationid.add(Integer.parseInt(value.toString())); //adding all incoming values to the list
                        }
                        int count=0; //creating variable count
                        for(int j=0;j<21;j++) //condition to loop through the keys in hashmap occupationmap
                        {
                                      for(Integer k: occupationid) //loop through elements in occupationid
                                      {
                                          if(k==j) //matching occupationid with key in occupationmap
                                          {
                                                  count++; //counting the occureneces
                                          }
	                                  }
                                occupationoccurences.put(occupationmap.get(j),count); //storing occupation name and its occurences in hashmap occupationoccurences
                                count=0; //resetting count
                        }
                        TreeMap<Integer,String> sortedoccupations = new TreeMap<Integer,String>(Collections.reverseOrder()); //Treemap sortedoccupations to sort the count in descending order
                        for (Entry<String, Integer> entry : occupationoccurences.entrySet()) //adding ocuupationoccurenecs to sortedoccupations by count as key
                        {
                            sortedoccupations.put(entry.getValue(),entry.getKey()); //count as key stored in sortedoccupations
                        }
                        int element=0;
                        int display=5;
                        for(Map.Entry<Integer,String> entry: sortedoccupations.entrySet()) 
                        {
                         if(element==display)break; //condition to dispaly top 5
                          context.write(new Text(entry.getValue()),new IntWritable(entry.getKey())); //generating output keyvalue pair
                          element++;
                        }

                }
        }
        public static void main(String[] args) throws Exception{
                Configuration conf=new Configuration();  //setting the configuration
                Job job=Job.getInstance(conf,"occupation count"); //setting the job
                job.setJarByClass(Vasanthi_Mudunuri_Program_1.class); //setting jar by class
                job.setMapperClass(OccupationMapper.class); //setting mapper class
                job.setReducerClass(OccupationReducer.class); //setting reducer class
                job.setOutputKeyClass(Text.class); //setting output key class
                job.setOutputValueClass(IntWritable.class); //setting output value class
                FileInputFormat.addInputPath(job,new Path(args[0])); //setting input path
                FileOutputFormat.setOutputPath(job,new Path(args[1])); //setting output path
                System.exit(job.waitForCompletion(true)? 0:1); //waiting for the job completion
        }
}
									  
                               