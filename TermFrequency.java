/* #################### pseeda@uncc.edu ####################
 * #################### Prabhakar Teja Seeda ###############
 */

//package org.myorg;

import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.*;


public class TermFrequency extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TermFrequency.class);

   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new TermFrequency(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " termfrequency ");
      job.setJarByClass( this .getClass());

      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( IntWritable .class);

      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();

      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");

      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
         Text currentWord  = new Text();
	 //Get the fileName using FileSplit
	 FileSplit fileSplit = (FileSplit)context.getInputSplit();
	 String fileName = fileSplit.getPath().getName(); 
	 System.out.println("The filename " + fileName); 
         for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }
	    //Converting the word to Lower case and concatenating the fileName in required format
	    word = word.toLowerCase().concat("#####").concat(fileName);	
            currentWord  = new Text(word);
            context.write(currentWord,one);
         }
      }
   }

   public static class Reduce extends Reducer<Text ,  IntWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         int sum  = 0;
         for ( IntWritable count  : counts) {
            sum  += count.get();
         }
	 //Calculating Each Word Frequency using the formula
	 double Wf;
	 if(sum > 0){
	    Wf = 1+(Math.log10(sum));	
         } else {
	    Wf = 0;	
	 }
	 //Write the Word and Frequency to the output file
         context.write(word,  new DoubleWritable(Wf));
      }
   }
}
