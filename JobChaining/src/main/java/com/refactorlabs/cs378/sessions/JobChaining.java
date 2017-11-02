package com.refactorlabs.cs378.sessions;

import java.util.List;
import java.util.HashMap;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.mapreduce.Job;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
import org.apache.avro.mapreduce.AvroKeyValueInputFormat;
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class JobChaining extends Configured implements Tool {
    
    //abstract mapper classes for the 5 seperate types
    public static abstract class SecondLevelMapper extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>> {
                
        @Override
        public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
        throws IOException, InterruptedException {

            EventSubtypeStatisticsKey.Builder keyBuilder = EventSubtypeStatisticsKey.newBuilder();
            EventSubtypeStatisticsData.Builder dataBuilder = EventSubtypeStatisticsData.newBuilder();
            HashMap<EventSubtype, Integer> eventSubtypeRecords = new HashMap<>();
            int total = 0;
            int temp = 0;

            Session sessions = value.datum();

            // iterate events and put value in event subtype records
            for (Event event : sessions.getEvents()) {
                if (eventSubtypeRecords.containsKey(event.getEventSubtype()))
                    eventSubtypeRecords.put(event.getEventSubtype(), eventSubtypeRecords.get(event.getEventSubtype()).intValue() + 1);
                else
                    eventSubtypeRecords.put(event.getEventSubtype(), 1);                       
                
            }
                
            // iterate event subtype records and write out event type and subtype data
            for (EventSubtype currentType : EventSubtype.values()) {
                temp = 0;
                EventSubtypeStatisticsKey.Builder thisKeyBuilder = EventSubtypeStatisticsKey.newBuilder();
                EventSubtypeStatisticsData.Builder thisDataBuilder = EventSubtypeStatisticsData.newBuilder();
                    
                thisDataBuilder.setSessionCount(1);

                if (eventSubtypeRecords.containsKey(currentType))
                    temp = eventSubtypeRecords.get(currentType).intValue();
                    
                // set key builder values
                thisKeyBuilder.setSessionType(getType());
                thisKeyBuilder.setEventSubtype(currentType.toString());
                //set data builder values
                thisDataBuilder.setTotalCount(temp);
                thisDataBuilder.setSumOfSquares(temp * temp);                    
                // write out
                context.write(new AvroKey<EventSubtypeStatisticsKey>(thisKeyBuilder.build()), new AvroValue<EventSubtypeStatisticsData>(thisDataBuilder.build()));
            }
            //loop to calculate all subtypes of all sessions, Extra Credit
            for (EventSubtype currentType : EventSubtype.values()) {
                temp = 0;

                dataBuilder.setSessionCount(1);

                if (eventSubtypeRecords.containsKey(currentType))
                    temp = eventSubtypeRecords.get(currentType).intValue();
                
                total += temp;
            }
            //set key builder value
            keyBuilder.setSessionType(getType());
            keyBuilder.setEventSubtype("any");
            //set data builder value
            dataBuilder.setTotalCount(total);
            dataBuilder.setSumOfSquares(total * total);

            //write out
            context.write(new AvroKey<EventSubtypeStatisticsKey>(keyBuilder.build()), new AvroValue<EventSubtypeStatisticsData>(dataBuilder.build()));
            
        }

        public abstract String getType();

    }
    
    //extended submitter mapper
    public static class SubmitterMapper extends SecondLevelMapper {
        @Override
        public String getType() {
            return SessionType.SUBMITTER.getText();
        }
    }

    //extended clicker mapper
    public static class ClickerMapper extends SecondLevelMapper {
        @Override
        public String getType() {
            return SessionType.CLICKER.getText();
        }
    }
    
    //extended shower mapper
    public static class ShowerMapper extends SecondLevelMapper {
        @Override
        public String getType() {
            return SessionType.SHOWER.getText();
        }
    }
    
    //extended visitor mapper
    public static class VisitorMapper extends SecondLevelMapper {
        @Override
        public String getType() {
            return SessionType.VISITOR.getText();
        }
    }
    //extended other mapper. There is no other event in this project input, adding this mapper and further jobs casued me path error in run()
    // public static class OtherMapper extends SecondLevelMapper {
    //     @Override
    //     public String getType() {
    //         return SessionType.OTHER.getText();
    //     }
    //}
    
    //AggregateReducer class    
    public static class AggregateReducer extends Reducer<AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>, AvroKey<EventSubtypeStatisticsKey>, AvroValue<EventSubtypeStatisticsData>> {
        
        public void reduce(AvroKey<EventSubtypeStatisticsKey> key, Iterable<AvroValue<EventSubtypeStatisticsData>> values, Context context) throws IOException, InterruptedException {
            
            int sessionCount = 0;
            int eventSubtypeCount = 0;
            int totalSumSquare = 0;
            double mean = 0;
            double variance = 0;
            
            //sum up all inputs in loop
            for (AvroValue<EventSubtypeStatisticsData> builder : values) {
                sessionCount += builder.datum().getSessionCount();
                eventSubtypeCount += builder.datum().getTotalCount();
                totalSumSquare += builder.datum().getSumOfSquares();
            }
 
            EventSubtypeStatisticsData.Builder finalBuilder = EventSubtypeStatisticsData.newBuilder();
            
            mean = (double)eventSubtypeCount / sessionCount;
            variance = (double)totalSumSquare / sessionCount - mean * mean;
            
            //set all the data values in builder
            finalBuilder.setMean(mean);
            finalBuilder.setVariance(variance);
            finalBuilder.setSessionCount(sessionCount);
            finalBuilder.setTotalCount(eventSubtypeCount);
            finalBuilder.setSumOfSquares(totalSumSquare);
            
            context.write(key, new AvroValue<EventSubtypeStatisticsData>(finalBuilder.build()));                       
        }
    }
    
    // The run() method is called (indirectly) from main(), and contains all the job
    // setup and configuration.
     
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: JobChaining <input path> <output path>");
            return -1;
        }
        
        Configuration conf = getConf();
        
        conf.setBoolean(MRJobConfig.MAPREDUCE_JOB_USER_CLASSPATH_FIRST, true);
        String[] appArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        
        // set the first mapper job
        Job filterAndBinJob = Job.getInstance(conf, "filterAndBinJob");
        filterAndBinJob.setJarByClass(JobChaining.class);
        filterAndBinJob.setMapperClass(SessionMapper.class);
        filterAndBinJob.setNumReduceTasks(0);
        filterAndBinJob.setInputFormatClass(AvroKeyValueInputFormat.class);
        filterAndBinJob.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        AvroJob.setInputKeySchema(filterAndBinJob, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(filterAndBinJob, Session.getClassSchema());
        AvroJob.setOutputKeySchema(filterAndBinJob, Schema.create(Schema.Type.STRING));
        AvroJob.setOutputValueSchema(filterAndBinJob, Session.getClassSchema());
        MultipleOutputs.setCountersEnabled(filterAndBinJob, true);
        FileInputFormat.setInputPaths(filterAndBinJob, appArgs[0]);
        FileOutputFormat.setOutputPath(filterAndBinJob, new Path(appArgs[1]));      
        
        for (SessionType thisType : SessionType.values()) {
            AvroMultipleOutputs.addNamedOutput(filterAndBinJob, thisType.getText(), AvroKeyValueOutputFormat.class, Schema.create(Schema.Type.STRING), Session.getClassSchema());
        }        
       //set wait for completion to true
        filterAndBinJob.waitForCompletion(true);
        
        //create job for submitter
        Job submitterJob = Job.getInstance(conf, "SubmitterJob");
        submitterJob.setJarByClass(JobChaining.class);
        submitterJob.setReducerClass(AggregateReducer.class);
        submitterJob.setInputFormatClass(AvroKeyValueInputFormat.class);
        submitterJob.setMapperClass(SubmitterMapper.class);
        submitterJob.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        submitterJob.setCombinerClass(AggregateReducer.class);
        submitterJob.setNumReduceTasks(1);
        AvroJob.setInputKeySchema(submitterJob, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(submitterJob, Session.getClassSchema());
        AvroJob.setMapOutputKeySchema(submitterJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(submitterJob, EventSubtypeStatisticsData.getClassSchema());
        AvroJob.setOutputKeySchema(submitterJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(submitterJob, EventSubtypeStatisticsData.getClassSchema());
        FileInputFormat.addInputPath(submitterJob, new Path(appArgs[1] + "/" + SessionType.SUBMITTER.getText() + "-m-*.avro"));
        FileOutputFormat.setOutputPath(submitterJob, new Path(appArgs[1] + "/" + SessionType.SUBMITTER.getText() + "Output"));
        submitterJob.submit();

        //create job for clicker
        Job clickerJob = Job.getInstance(conf, "ClickerJob");
        clickerJob.setJarByClass(JobChaining.class);
        clickerJob.setInputFormatClass(AvroKeyValueInputFormat.class);
        clickerJob.setMapperClass(ClickerMapper.class);
        clickerJob.setReducerClass(AggregateReducer.class);
        clickerJob.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        clickerJob.setCombinerClass(AggregateReducer.class);
        clickerJob.setNumReduceTasks(1);
        AvroJob.setInputKeySchema(clickerJob, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(clickerJob, Session.getClassSchema());
        AvroJob.setMapOutputKeySchema(clickerJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(clickerJob, EventSubtypeStatisticsData.getClassSchema());
        AvroJob.setOutputKeySchema(clickerJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(clickerJob, EventSubtypeStatisticsData.getClassSchema());
        FileInputFormat.addInputPath(clickerJob, new Path(appArgs[1] + "/" + SessionType.CLICKER.getText() + "-m-*.avro"));
        FileOutputFormat.setOutputPath(clickerJob, new Path(appArgs[1] + "/" + SessionType.CLICKER.getText() + "Output"));
        clickerJob.submit();

        //create job for shower
        Job showerJob = Job.getInstance(conf, "ShowerJob");
        showerJob.setJarByClass(JobChaining.class);
        showerJob.setInputFormatClass(AvroKeyValueInputFormat.class);
        showerJob.setMapperClass(ShowerMapper.class);
        showerJob.setReducerClass(AggregateReducer.class);
        showerJob.setOutputFormatClass(AvroKeyValueOutputFormat.class);    
        showerJob.setCombinerClass(AggregateReducer.class);
        showerJob.setNumReduceTasks(1);
        AvroJob.setInputKeySchema(showerJob, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(showerJob, Session.getClassSchema());
        AvroJob.setMapOutputKeySchema(showerJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(showerJob, EventSubtypeStatisticsData.getClassSchema());
        AvroJob.setOutputKeySchema(showerJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(showerJob, EventSubtypeStatisticsData.getClassSchema());
        FileInputFormat.addInputPath(showerJob, new Path(appArgs[1] + "/" + SessionType.SHOWER.getText() + "-m-*.avro"));
        FileOutputFormat.setOutputPath(showerJob, new Path(appArgs[1] + "/" + SessionType.SHOWER.getText() + "Output"));
        showerJob.submit();
        
        
        //create job for visitor
        Job visitorJob = Job.getInstance(conf, "VisitorJob");
        visitorJob.setJarByClass(JobChaining.class);
        visitorJob.setInputFormatClass(AvroKeyValueInputFormat.class);
        visitorJob.setMapperClass(VisitorMapper.class);
        visitorJob.setReducerClass(AggregateReducer.class);
        visitorJob.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        visitorJob.setCombinerClass(AggregateReducer.class);
        visitorJob.setOutputFormatClass(AvroKeyValueOutputFormat.class);
        visitorJob.setCombinerClass(AggregateReducer.class);
        visitorJob.setNumReduceTasks(1);
        AvroJob.setInputKeySchema(visitorJob, Schema.create(Schema.Type.STRING));
        AvroJob.setInputValueSchema(visitorJob, Session.getClassSchema());
        AvroJob.setMapOutputKeySchema(visitorJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(visitorJob, EventSubtypeStatisticsData.getClassSchema());
        AvroJob.setOutputKeySchema(visitorJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(visitorJob, EventSubtypeStatisticsData.getClassSchema());
        FileInputFormat.addInputPath(visitorJob, new Path(appArgs[1] + "/" + SessionType.VISITOR.getText() + "-m-*.avro"));
        FileOutputFormat.setOutputPath(visitorJob, new Path(appArgs[1] + "/" + SessionType.VISITOR.getText() + "Output"));
        visitorJob.submit();
        
        while (!(submitterJob.isComplete()&&showerJob.isComplete()&&clickerJob.isComplete()&&visitorJob.isComplete())) {
            //thread keeps sleeping for 15s if not all the jobs done
            Thread.sleep(15000);
        }
        
        //create job for final aggregating
        Job aggregateJob = Job.getInstance(conf, "AggregateJob");
        aggregateJob.setJarByClass(JobChaining.class);
        aggregateJob.setInputFormatClass(AvroKeyValueInputFormat.class);
        aggregateJob.setMapperClass(Mapper.class);
        aggregateJob.setCombinerClass(AggregateReducer.class);
        aggregateJob.setReducerClass(AggregateReducer.class);
        aggregateJob.setOutputFormatClass(TextOutputFormat.class);
        aggregateJob.setNumReduceTasks(1);
        AvroJob.setInputKeySchema(aggregateJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setInputValueSchema(aggregateJob, EventSubtypeStatisticsData.getClassSchema());
        AvroJob.setMapOutputKeySchema(aggregateJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setMapOutputValueSchema(aggregateJob, EventSubtypeStatisticsData.getClassSchema());
        AvroJob.setOutputKeySchema(aggregateJob, EventSubtypeStatisticsKey.getClassSchema());
        AvroJob.setOutputValueSchema(aggregateJob, EventSubtypeStatisticsData.getClassSchema());
        FileInputFormat.addInputPath(aggregateJob, getInputPath(appArgs[1], SessionType.SUBMITTER.getText()));       
        FileInputFormat.addInputPath(aggregateJob, getInputPath(appArgs[1], SessionType.CLICKER.getText()));
        FileInputFormat.addInputPath(aggregateJob, getInputPath(appArgs[1], SessionType.SHOWER.getText()));
        FileInputFormat.addInputPath(aggregateJob, getInputPath(appArgs[1], SessionType.VISITOR.getText()));
        FileOutputFormat.setOutputPath(aggregateJob, new Path(appArgs[1] + "/aggregateJobOutput"));
        
        aggregateJob.waitForCompletion(true);
        
        return 0;
    }
    
    //method to create input paths for reducer
    public static Path getInputPath(String part1, String part2) {
        return new Path(part1 + "/" + part2 + "Output/part-r-*.avro");
    }
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new JobChaining(), args);
        System.exit(res);
    }
}
