/* #################### pseeda@uncc.edu ####################
 * #################### Prabhakar Teja Seeda ###############
 */

//package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;
import java.util.*;


public class Search extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( Search.class);

   public static void main( String[] args) throws  Exception {
      //Run Search Job
      int res  = ToolRunner .run( new Search(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      String queryWords = args[2];
      //Initializing the ConfigurationObject
      Configuration config = getConf();
      FileSystem fileSystem = FileSystem.get(config);
      //Saving the query of the user from the input arguments
      config.set("query",queryWords);
      Job job  = Job .getInstance(getConf(), " search ");
      job.setJarByClass( this .getClass());
      //Input path is the output path from the TFIDF job
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setMapOutputKeyClass(Text. class);
      job.setMapOutputValueClass(DoubleWritable. class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }

   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  DoubleWritable > {


      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
         String userQuery = context.getConfiguration().get("query");
         LOG.info("The querywords from config are "+userQuery);
         String[] queryWords = userQuery.toLowerCase().split(" ");
         LOG.info("The querywords are "+queryWords);
         //Split the input with respect to '#####' delimiter from TFIDF output
	       //to access FileName and TFIDF value
         String wordName = line.split("#####")[0];
         String term = line.split("#####")[1];
         //FileName
         String fileName = term.split("\t")[0];
         //TFIDF value
         Double tf_idf = Double.parseDouble(term.split("\t")[1]);
         for(String word : queryWords){
            if(word.equals(wordName)){
              //Writing the fileName and tf_idf values to the context
              context.write(new Text(fileName),new DoubleWritable(tf_idf));
            }
         }
      }
   }

   public static class Reduce extends Reducer<Text ,  DoubleWritable ,  Text ,  DoubleWritable > {
      @Override
      public void reduce( Text word,  Iterable<DoubleWritable > tfidf_scores,  Context context)
         throws IOException,  InterruptedException {
         double tf_idf_score = 0;
         for(DoubleWritable value : tfidf_scores){
           //Calculating the sum of tf_idf score for the filename
           tf_idf_score += value.get();
         }
         //Writing the filename and tf_idf total score
         context.write(word, new DoubleWritable(tf_idf_score));
      }
   }
}
