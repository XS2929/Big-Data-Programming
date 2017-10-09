package com.refactorlabs.cs378.assign2;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;
import com.refactorlabs.cs378.assign2.WordStatisticsWritable;
/**
 * Example MapReduce program that performs word count.
 *
 * @author BULBASAUR
 */
public class WordStatistics extends Configured implements Tool {    
    /**
     * The Map class for word count.  Extends class Mapper, provided by Hadoop.
     * This class defines the map() function for the word count example.
     */
    public static class MapClass extends Mapper<LongWritable, Text, Text, WordStatisticsWritable> {

        /**
         * Local variable "word" will contain the word identified in the input.
         * The Hadoop Text object is mutable, so we can reuse the same object and
         * simply reset its value as each word in the input is encountered.
         */
        private Text word = new Text();
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();
            String remove = line.replace("["," [");
            String remove1 = remove.replace("--"," ");
            String newLine = remove1.replace("]","] ");
            StringTokenizer tokenizer = new StringTokenizer(newLine," .!?,_:;\"");
            Hashtable<String,Long> words = new Hashtable<String,Long>();

            while (tokenizer.hasMoreTokens()) {
                String currentWord = tokenizer.nextToken();
                
                if(words.containsKey(currentWord)){
                    words.put(currentWord,words.get(currentWord)+1L);
                }
                else{
                    words.put(currentWord,1L);
                }
            }
            Set<String> keys = words.keySet();
            for(String tempKey: keys){
                WordStatisticsWritable writable = new WordStatisticsWritable();
                word.set(tempKey);
                writable.setParagraphCount(1L);
                writable.setWordCount(words.get(tempKey));
                writable.setWordCountSquare((words.get(tempKey))*(words.get(tempKey)));
                context.write(word,writable);
            }

        }
    }
    public static class Combiner extends Reducer<Text, WordStatisticsWritable, Text, WordStatisticsWritable> {
        @Override
        public void reduce(Text key, Iterable<WordStatisticsWritable> values, Context context)
                throws IOException, InterruptedException {
            long tempParagraphCountSum  = 0;
            long tempWordCountSquareSum =0;
            long tempWordCountSum = 0;
            for(WordStatisticsWritable value: values){
                tempParagraphCountSum += value.getParagraphCount();
                tempWordCountSum += value.getWordCount();
                tempWordCountSquareSum += value.getWordCountSquare();
            } 
            WordStatisticsWritable writable = new WordStatisticsWritable();
            writable.setParagraphCount(tempParagraphCountSum);
            writable.setWordCount(tempWordCountSum);
            writable.setWordCountSquare(tempWordCountSquareSum);
            writable.setMean(0);
            writable.setVariance(0);

            context.write(key,writable);            

        }
    }
        


    /**
     * The Reduce class for word count.  Extends class Reducer, provided by Hadoop.
     * This class defines the reduce() function for the word count example.
     */
    public static class ReduceClass extends Reducer<Text, WordStatisticsWritable, Text, WordStatisticsWritable> {
        @Override
        public void reduce(Text key, Iterable<WordStatisticsWritable> values, Context context)
                throws IOException, InterruptedException {
            long tempParagraphCountSum  = 0;
            long tempWordCountSquareSum =0;
            long tempWordCountSum = 0;
            for(WordStatisticsWritable value: values){
                tempParagraphCountSum += value.getParagraphCount();
                tempWordCountSum += value.getWordCount();
                tempWordCountSquareSum += value.getWordCountSquare();
            } 
            double mean = calculateMean(tempParagraphCountSum, tempWordCountSum);
            double variance = calculateVariance(tempParagraphCountSum, mean, tempWordCountSquareSum);
            WordStatisticsWritable writable = new WordStatisticsWritable();
            writable.setParagraphCount(tempParagraphCountSum);
            writable.setWordCount(tempWordCountSum);
            writable.setWordCountSquare(tempWordCountSquareSum);
            writable.setMean(mean);
            writable.setVariance(variance);

            context.write(key,writable);            

        }
        public double calculateMean(long paragraphCountSum, long wordCountSum){
            return (double)wordCountSum/(double)paragraphCountSum;
        }
        public double calculateVariance(long paragraphCountSum, double mean, double wordCountSquareSum){
            return (double)wordCountSquareSum/(double)paragraphCountSum - mean*mean;
        }
    }

    /**
     * The run method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        System.setProperty("hadoop.home.dir", "/");
        Job job = Job.getInstance(conf, "WordStatistics");
        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(WordStatistics.class);

        // Set the output key and value types (for map and reduce).
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(WordStatisticsWritable.class);

        // Set the map and reduce classes.
        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);
        job.setCombinerClass(Combiner.class);

        // Set the input and output file formats.
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Grab the input file and output directory from the command line.
        FileInputFormat.addInputPath(job, new Path(appArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        Utils.printClassPath();
        //BasicConfigurator.configure();
        System.setProperty("hadoop.home.dir", "/");
        int res = ToolRunner.run(new WordStatistics(), args);
        System.exit(res);
    }
}


