package com.refactorlabs.cs378.sessions;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
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
 * 
 */
public class FilteringAndMultipleOutputs extends Configured implements Tool {


    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: FilteringAndMultipleOutputs <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        conf.set("mapreduce.user.classpath.first", "true");
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);

        Job job = Job.getInstance(conf, "FilteringAndMultipleOutputs");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(FilteringAndMultipleOutputs.class);
        job.setInputFormatClass(AvroKeyValueInputFormat.class);
        // Specify the Map
        job.setMapperClass(SessionMapper.class);
        AvroJob.setMapOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());
        // Specify the Reduce
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setNumReduceTasks(0);

        AvroJob.setInputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(job, Session.getClassSchema());
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Session.getClassSchema());
        
        AvroMultipleOutputs.addNamedOutput(job, SessionType.SUBMITTER.getText(), AvroKeyValueOutputFormat.class,Schema.create(Schema.Type.STRING), Session.getClassSchema());
        AvroMultipleOutputs.addNamedOutput(job, SessionType.CLICKER.getText(), AvroKeyValueOutputFormat.class,Schema.create(Schema.Type.STRING), Session.getClassSchema());
        AvroMultipleOutputs.addNamedOutput(job, SessionType.SHOWER.getText(), AvroKeyValueOutputFormat.class,Schema.create(Schema.Type.STRING), Session.getClassSchema());
        AvroMultipleOutputs.addNamedOutput(job, SessionType.VISITOR.getText(), AvroKeyValueOutputFormat.class,Schema.create(Schema.Type.STRING), Session.getClassSchema());
        AvroMultipleOutputs.addNamedOutput(job, SessionType.OTHER.getText(), AvroKeyValueOutputFormat.class,Schema.create(Schema.Type.STRING), Session.getClassSchema());
        


        AvroMultipleOutputs.setCountersEnabled(job, true);
        FileInputFormat.addInputPath(job, new Path(appArgs[0]));
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
        int res = ToolRunner.run(new FilteringAndMultipleOutputs(), args);
        System.exit(res);
    }

}
