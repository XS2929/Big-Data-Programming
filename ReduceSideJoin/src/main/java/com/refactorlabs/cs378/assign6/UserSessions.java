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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
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
 * UserSessions using AVRO defined class for the UserSessions data,
 */
public class UserSessions extends Configured implements Tool {

    /**
     * The Reduce class for WordStatistics.  Extends class Reducer, provided by Hadoop.
     * This class defines the reduce() function for the WordStatistics.
     */
    public static class ReduceClass extends Reducer<Text, AvroValue<Session>,
            AvroKey<CharSequence>, AvroValue<Session>> {

        @Override
        public void reduce(Text key, Iterable<AvroValue<Session>> values, Context context)
                throws IOException, InterruptedException {
            //initialize builders
            Session.Builder sessionBuilder = Session.newBuilder(); 
            CharSequence userID = key.toString();
            sessionBuilder.setUserId(userID);
            //initialize event list
            List<Event> eventList = new ArrayList<Event>();

            //iterate through inputed Session list
            for (AvroValue<Session> value : values) {
                List<Event> tempEvent= value.datum().getEvents();
                boolean unique = true;
                //check whether the event of session already exist  in eventList, uniqueness check
                //if not unique add it to event list, else ignore it
                for (Event event : eventList){
                    if(tempEvent.get(0).equals(event))
                        unique = false;
                }
                if(unique)
                    eventList.add(tempEvent.get(0));
                }
            //sort events in order using helper function, then feed it to builder
            sessionBuilder.setEvents(sortEventList(eventList));

            context.write(new AvroKey<CharSequence>(key.toString()), new AvroValue<Session>(sessionBuilder.build()));
        }
        //the function to sort event builder, using self-defined comparable class
        private List<Event> sortEventList (List<Event> mylist){
            List<ComparableEvent> comparableList = new ArrayList<ComparableEvent>();
            List<Event> toReturn = new ArrayList<Event>();
            //store the events to comparable list first
            for (int i = 0; i < mylist.size(); i++){
                ComparableEvent tempEvent = new ComparableEvent(mylist.get(i));
                comparableList.add(tempEvent);
            }
            //sort the events
            Collections.sort(comparableList);

            //store the sorted events to a event list
            for (int i =0; i <  comparableList.size(); i++)
                toReturn.add(i,comparableList.get(i).getEvent());

            return toReturn;
        }
    }



    /**
     * The run() method is called (indirectly) from main(), and contains all the job
     * setup and configuration.
     */
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: UserSessions <input path> <output path>");
            return -1;
        }

        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "UserSessions");
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Identify the JAR file to replicate to all machines.
        job.setJarByClass(UserSessions.class);
        //conf.set("mapreduce.user.classpath.first", "true");

        // Specify the Map
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(UserSessionsMapper.class);
        job.setMapOutputKeyClass(Text.class);
        AvroJob.setMapOutputValueSchema(job, Session.getClassSchema());

        // Specify the Reduce
        job.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        job.setReducerClass(UserSessions.ReduceClass.class);
        //AvroJob.setOutputKeySchema(job,
                //CharSequence.getCharSquareSchema(Schema.create(Schema.Type.STRING), Session.getClassSchema()));
        //job.setOutputValueClass(Session.class);
        AvroJob.setOutputKeySchema(job, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(job, Session.getClassSchema());


        //job.setOutputKeyClass(Text.class);
        //AvroJob.setOutputValueSchema(job, Session.getClassSchema());

        // Grab the input file and output directory from the command line.
        FileInputFormat.addInputPaths(job, appArgs[0]);
        
        FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

        String[] inputPaths = appArgs[0].split(",");
        for ( String inputPath : inputPaths ) {
            FileInputFormat.addInputPath(job, new Path(inputPath));
        }
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
        int res = ToolRunner.run(new UserSessions(), args);
        System.exit(res);
    }

}
