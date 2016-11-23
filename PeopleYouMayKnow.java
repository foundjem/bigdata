package mapReduce_hadoop;

import java.io.IOException;
import java.util.*;
import java.lang.Math;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
        
public class PeopleYouMayKnow {
/* From the input file given its nature: <user> <TAB> <friends of users> 
for each readLine of input, generates pair of common firnds for user or existing friends  */
  public static class Map1 extends Mapper<LongWritable, Text, Text, IntWritable> {
    // common_friends indicates there is a common friends beTween two users 
    // already_friends indicates these two users are already friends
    private final static IntWritable common_friends = new IntWritable(1);  
    private final static IntWritable already_friends = new IntWritable(0); 
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String readLine = value.toString();
      int reference = readLine.indexOf('\t');  //TAB
      if(reference == -1) 
       return;        
      String userKey = readLine.substring(0, reference);
      Vector<String> friendsID = new Vector<String>();
      StringTokenizer toks = new StringTokenizer(readLine.substring(reference+1), ""+',');
      while(toks.hasMoreTokens()) {
       friendsID.add(toks.nextToken());
      }
      int length = friendsID.size(), i=0,j=0;
     
      while(i<length){
        context.write(new Text(userKey + ',' + friendsID.elementAt(i)), already_friends);
        while(j<length){ 
           if(j == i)
             continue;
          context.write(new Text(friendsID.elementAt(i) + ',' + friendsID.elementAt(j)), common_friends);
        }
        i++;
        j++;
      }//END WHILE
    }
  } 
  /* THE PURO OF THIS FIRST REDUCER IS TO COUNT COMMON FRIENDS FOR EACH IUSER */       
  public static class Reduce1 extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0, prod = 1;
      for (IntWritable val : values) {
        sum += val.get();
        prod *= val.get();
      }
      if( prod!=0 )
        context.write(key, new IntWritable(sum));
    }
  }
 
  /* THE FIRST ITEM FROM THE FILE IS READ AS THRE USER ID, AND IT IS USED TO MAP EACH VALUE FROM OF THE READLINE S PAIR VALUES */
  
  public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String readLine = value.toString();
      
      int reference, reference2;
      if((reference = readLine.indexOf(',')) == -1) 
       return;
      if((reference2 = readLine.indexOf('\t')) == -1)
       return;
      String userKey = readLine.substring(0, reference);
      String friend_id = readLine.substring(reference+1, reference2);
      String commonFriend = readLine.substring(reference2+1);
      context.write(new Text(userKey), new Text(friend_id + ',' + commonFriend));
    }
  }
  /* HERE WE ARE LOOKING FOR THE 10 MOST COMMON FRIENDS TO THE USER */
  public static class Reduce2 extends Reducer<Text, Text, Text, Text> {
   
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      Vector<int[]> popular  = new Vector<int[]>();
      //THE IDs OF FRIENDS ARE ADDED HERE SORTED
      for (Text val : values) {
        String val_str = val.toString();
        int reference;
        if((reference = val_str.indexOf(',')) == -1)
          return;
        int[] combine = new int[2];
        combine[0] = Integer.parseInt(val_str.substring(0, reference));
        combine[1] = Integer.parseInt(val_str.substring(reference+1));
      
        if(popular.isEmpty())
          popular.insertElementAt(combine,0);
        else {
          int i;
          for(i = 0; i < Math.min(popular.size(), 10); i ++) {
            if(popular.get(i)[1] < combine[1] ||
            (popular.get(i)[1] == combine[1] && popular.get(i)[0] > combine[0])) {
              popular.insertElementAt(combine, i);
              while(popular.size() > 10)
                popular.removeElementAt(popular.size()-1);
              break;
            }
          }
          if( i == popular.size() && i < 10)
            popular.add(combine);
        }//END ELSE
      }
      String Strpopular = "";
      int i=0;
      while(i < popular.size()){
        Strpopular += Integer.toString(popular.get(i)[0]);
        if(i != popular.size()-1)
          Strpopular += ',';
        i++;
      }
      context.write(key, new Text(Strpopular));
    }  
  }

  @SuppressWarnings("deprecation")
public static void main(String[] args) throws Exception {
    // --------- CREATING THE FIRST MAPREDUCE JOB-------------------------
    // JOB ONE COMPUTES THE MUTUAL FRIENDS OF USER
    Configuration conf = new Configuration();

    Job job = new Job(conf, "PeopleYouMayKnow-MapReduce-1");
    job.setJarByClass(PeopleYouMayKnow.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);

    job.setMapperClass(Map1.class);
    job.setReducerClass(Reduce1.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));  //INPUT FROM FILE
    FileOutputFormat.setOutputPath(job, new Path(args[1])); // OUTPUT TO MR TWO

    job.waitForCompletion(true);
    // --------  CREATING THE SECOND MAPREDUCE JOB -------
    // COMPUTES THE TOP N RECOOMENDATION FOR USER

    Configuration conf2 = new Configuration();

    Job job2 = new Job(conf2, "PeopleYouMayKnow-MapReduce-2");
    job2.setJarByClass(PeopleYouMayKnow.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);

    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(Text.class);

    job2.setMapperClass(Map2.class);
    job2.setReducerClass(Reduce2.class);

    job2.setInputFormatClass(TextInputFormat.class);
    job2.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job2, new Path(args[2]));  //OUTPUT OF MR1 FEEDS IN AS INPUT ARGS HERE
    FileOutputFormat.setOutputPath(job2, new Path(args[3])); // FINAL OUTPUT GOES HERE

    job2.waitForCompletion(true);
  }    

}
