package com.refactorlabs.cs378.assign6;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.Schema;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.avro.mapred.Pair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import com.google.common.collect.Lists;


import java.io.IOException;
import java.util.*;
import java.util.Collections;

/**
 * ReduceSideJoin using AVRO defined class for the ReduceSideJoin data,
 */
public class ReduceSideJoin extends Configured implements Tool {

    /**
     * The Reduce class for ReduceSideJoin.  Extends class Reducer, provided by Hadoop.
     * This class defines the reduce() function for the WordStatistics.
     */
    public static class ReduceClass extends Reducer<Text, AvroValue<VinImpressionCounts>,
            Text, AvroValue<VinImpressionCounts>> {

        @Override
        public void reduce(Text key, Iterable<AvroValue<VinImpressionCounts>> values, Context context)
                throws IOException, InterruptedException {
            //initialize builders
            VinImpressionCounts.Builder vinImpressionCountsBuilder = VinImpressionCounts.newBuilder(); 
            Text word = new Text();
            word.set(key.toString());
            //initialize the click map which will be put to builder
            Map<CharSequence, Long> clickMap = new TreeMap<CharSequence, Long>();
            //long values record unique users, srp value, vdp value and edit contact form count
            long uniqueUsers = 0, marketplaceSrps = 0, marketplaceVdps = 0, editContactForm = 0;

            //iterate through inputed VinImpressionCounts list
            for (AvroValue<VinImpressionCounts> value : values) {
                //sum up the unique user count value
                uniqueUsers += value.datum().getUniqueUsers();
                //sum up the srp value
                marketplaceSrps += value.datum().getMarketplaceSrps();
                //sum up the vdp value
                marketplaceVdps += value.datum().getMarketplaceVdps();
                //sum up the edit contact form count
                editContactForm += value.datum().getEditContactForm();
                //if the click list of current VinImpressionCounts is not empty, merge with our click map for building
                //using the Java8(Python style) method forEach()
                if(!value.datum().getClicks().isEmpty())
                    value.datum().getClicks().forEach((k, v) -> clickMap.merge(k, v, (v1, v2) -> v1 + v2));

            }
            //since this project is left join, write out this VIN only when the unique user count is not zero
            if(uniqueUsers > 0){
                //set the filds and build
                vinImpressionCountsBuilder.setUniqueUsers(uniqueUsers);
                vinImpressionCountsBuilder.setClicks(clickMap);
                vinImpressionCountsBuilder.setEditContactForm(editContactForm);
                vinImpressionCountsBuilder.setMarketplaceSrps(marketplaceSrps);
                vinImpressionCountsBuilder.setMarketplaceVdps(marketplaceVdps);
                //write out
                context.write(word, new AvroValue<VinImpressionCounts>(vinImpressionCountsBuilder.build()));
            }
        }
        
    }



    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: ReduceSideJoin <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(conf, "ReduceSideJoin");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(ReduceSideJoin.class);

        // Specify the Map
        job.setMapOutputKeyClass(Text.class);
        //job.setMapperClass(UserSessionsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        //AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));        
        AvroJob.setMapOutputValueSchema(job, VinImpressionCounts.getClassSchema());

        // Specify the Reduce
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setReducerClass(ReduceSideJoin.ReduceClass.class);
        job.setOutputKeyClass(Text.class);
        AvroJob.setOutputValueSchema(job, VinImpressionCounts.getClassSchema());
        job.setOutputKeyClass(Text.class);


        // Grab the input file and output directory from the command line.
        //FileInputFormat.addInputPaths(job, appArgs[0]);
        String[] inputPaths = appArgs[0].split(",");
        MultipleInputs.addInputPath(job, new Path(inputPaths[0]),TextInputFormat.class,CSVMapper.class);
        MultipleInputs.addInputPath(job, new Path(inputPaths[1]),TextInputFormat.class,CSVMapper.class);
        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job, Session.getClassSchema());
        MultipleInputs.addInputPath(job, new Path(inputPaths[2]),AvroKeyValueInputFormat.class,SessionMapper.class);

        
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

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
        int res = ToolRunner.run(new ReduceSideJoin(), args);
        System.exit(res);
    }

}
