/* #################### pseeda@uncc.edu ####################
 * #################### Prabhakar Teja Seeda ###############
 */

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


public class TFIDF extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( TFIDF.class);

   public static void main( String[] args) throws  Exception {
      //String[] term_freq_args = {args[0],"TermFreqOutput"};
      //Run Term Frequency Job
      //int termfreqRes = ToolRunner .run( new TermFrequency(), term_freq_args);
      //Run TFIDF Job
      int res  = ToolRunner .run( new TFIDF(), args);
      //System .exit(termfreqRes);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      //Initializing the ConfigurationObject
      Configuration config = getConf();
      FileSystem fileSystem = FileSystem.get(config);
      //Job to calculate the Term Frequency
      Job termJob  = Job .getInstance(getConf(), " termfrequency ");
      termJob.setJarByClass( this .getClass());

      FileInputFormat.addInputPaths(termJob,  args[0]);
      //Intermediate Path which acts as output path for Term Frequency
      FileOutputFormat.setOutputPath(termJob, new Path("TermFreqOutput"));
      //Setting the Map and Reduce classes for the termJob
      termJob.setMapperClass( TermMap .class);
      termJob.setReducerClass( TermReduce .class);
      termJob.setOutputKeyClass( Text .class);
      termJob.setOutputValueClass( IntWritable .class);
      termJob.waitForCompletion( true);
      //Waits until the termJob completes to proceed to tfidfJob

      //Storing the number of files in the config to use them in Map, Reduce functions
      final int totalFiles = fileSystem.listStatus(new Path(args[0])).length;
      config.setInt("totalFiles",totalFiles);
      Job tfidfJob  = Job .getInstance(getConf(), " tfidf ");
      tfidfJob.setJarByClass( this .getClass());
      //Set the Output folder of the termJob as input folder to the tfidfJob
      FileInputFormat.addInputPaths(tfidfJob, "TermFreqOutput");
      FileOutputFormat.setOutputPath(tfidfJob,  new Path(args[ 1]));
      //Setting the Map and Reduce classes for tfidfJob
      tfidfJob.setMapperClass( Map .class);
      tfidfJob.setReducerClass( Reduce .class);
      tfidfJob.setMapOutputKeyClass(Text. class);
      tfidfJob.setMapOutputValueClass(Text. class);
      tfidfJob.setOutputKeyClass( Text .class);
      tfidfJob.setOutputValueClass( Text .class);
      //Returns after finishing the tfidfJob
      return tfidfJob.waitForCompletion( true)  ? 0 : 1;
   }

   //Map function for Term Frequency Job
   public static class TermMap extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
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

   //Reduce Function for Term Frequency Job
   public static class TermReduce extends Reducer<Text ,  IntWritable ,  Text ,  DoubleWritable > {
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


   //Map function for TFIDF job
   public static class Map extends Mapper<LongWritable ,  Text ,  Text ,  Text > {


      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

         String line  = lineText.toString();
         Text keyWord  = new Text();
         Text valueWord = new Text();
         //Split the input with respect to '#####' delimiter and access the Filename and Term Frequency
         String wordName = line.split("#####")[0];
         String term = line.split("#####")[1];
         //System.out.println("The term is "+ term);
         String fileName = term.split("\t")[0];
         //Performing the operation to convert the text into required format for the key,value pair
         String value_term = fileName + "=" + term.split("\t")[1];
         keyWord = new Text(wordName);
         valueWord = new Text(value_term);
         LOG.info("The keyword is "+keyWord);
	       LOG.info("The valueword is "+valueWord);
         context.write(keyWord,valueWord);
      }
   }


   //Reduce FUnction for TFIDF job
   public static class Reduce extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
      @Override
      public void reduce( Text word,  Iterable<Text > valueWords,  Context context)
         throws IOException,  InterruptedException {
         //Get the number of files in the input folder from configuration attribute
         int totalFiles = context.getConfiguration().getInt("totalFiles",0);
         LOG.info("The totalfiles is "+totalFiles);
         double tf_idf, idf;
         List<String> fileName = new ArrayList<String>();
         List<Double> frequency = new ArrayList<Double>();
         //Loop to store fileNames and word Names in array list
         for(Text words : valueWords){
            String[] x = words.toString().split("=");
            //Saving the number of files in which the word exists
            fileName.add(x[0]);
            //Saving the weighed term frequency of the word inside the file
            frequency.add(Double.parseDouble(x[1]));
         }
         //Calculating IDF for each word using the formula
         idf = Math.log10(1+(totalFiles/fileName.size()));
         //Calculating TFIDF for each word in different files
         for(int i=0;i<fileName.size();i++){
            tf_idf = idf * frequency.get(i);
            //Write the word and fileName with the calculated TFIDF value
            context.write(new Text(word.toString().concat("#####")
           			    .concat(fileName.get(i))), new DoubleWritable(tf_idf));
         }
      }
   }


}
