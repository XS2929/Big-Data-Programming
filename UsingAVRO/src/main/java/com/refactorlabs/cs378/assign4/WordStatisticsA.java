package com.refactorlabs.cs378.assign4;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * WordStatistics using AVRO defined class for the word count data,
 */
public class WordStatisticsA extends Configured implements Tool {

    /**
     * The Reduce class for WordStatistics.  Extends class Reducer, provided by Hadoop.
     * This class defines the reduce() function for the WordStatistics.
     */
    public static class ReduceClass extends Reducer<Text, AvroValue<WordStatisticsData>,
            Text, AvroValue<WordStatisticsData>> {

        @Override
        public void reduce(Text key, Iterable<AvroValue<WordStatisticsData>> values, Context context)
                throws IOException, InterruptedException {
            long tempDocumentCount= 0, tempTotalCount = 0, tempMin = 100000000, tempMax = 0, tempSumOfSquares = 0;
            double tempMean = 0.0, tempVariance = 0.0;
            //loop through the WordStatisticsDatas, calculate sum of squares, max, min, word count and document count
            for (AvroValue<WordStatisticsData> value : values) {
                tempDocumentCount += value.datum().getDocumentCount();
                tempTotalCount += value.datum().getTotalCount();
                if (value.datum().getTotalCount() >= tempMax){
                    tempMax = value.datum().getTotalCount();
                }
                if (value.datum().getTotalCount() <= tempMin){
                    tempMin = value.datum().getTotalCount();
                }
                tempSumOfSquares += value.datum().getSumOfSquares();
            }
            //calculate mean and variance
            tempMean = (double)tempTotalCount/(double)tempDocumentCount;
            tempVariance = (double)tempSumOfSquares/(double)tempDocumentCount - (double)(tempMean*tempMean);
            // Emit the total count for the word.
            WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();
            
            builder.setDocumentCount(tempDocumentCount);
            builder.setTotalCount(tempTotalCount);
            builder.setMin(tempMin);
            builder.setMax(tempMax);
            builder.setSumOfSquares(tempSumOfSquares);
            builder.setMean(tempMean);
            builder.setVariance(tempVariance);
 
            context.write(key,new AvroValue<WordStatisticsData>(builder.build()));        
        }
    }



    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordStatisticsA <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "WordStatisticsA");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(WordStatisticsA.class);

        // Specify the Map
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(WordStatisticsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, WordStatisticsData.getClassSchema());

        // Specify the Reduce
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(ReduceClass.class);
        job.setOutputKeyClass(Text.class);
        AvroJob.setOutputValueSchema(job, WordStatisticsData.getClassSchema());

        // Grab the input file and output directory from the command line.
        FileInputFormat.addInputPaths(job, appArgs[0]);
        
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

        // Initiate the map-reduce job, and wait for completion.
        job.waitForCompletion(true);

        return 0;
    }

    /**
     * The main method specifies the characteristics of the map-reduce job
     * by setting values on the Job object, and then initiates the map-reduce
     * job and waits for it to complete.
     */
    public static void main(String[] args) throws Exception {
        Utils.printClassPath();
        int res = ToolRunner.run(new WordStatisticsA(), args);
        System.exit(res);
    }

}
