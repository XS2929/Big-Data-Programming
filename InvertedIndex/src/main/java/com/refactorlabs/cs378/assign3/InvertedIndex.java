package com.refactorlabs.cs378.assign3;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
import java.util.StringTokenizer;  
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Inverted Index Assignment
 *
 * @author BULBASAUR
 */
public class InvertedIndex extends Configured implements Tool {

	/**
	 * The Map class for Inverted Index.  Extends class Mapper, provided by Hadoop.
	 * This class defines the map() function.
	 */
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {

		/**
		 * Read in a line fo string, use regex to catch the needed substrings for creating key/value pairs
		 * key can be From:email, To:email, Bcc:email and Cc:email. Values is uniform, which is the sender id
		 */
		private Text uniformOutput = new Text();

		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//remove the parts of useless substring
			String line = value.toString().replace("\\"," ").replace("/"," ").replace("\""," ").replace("\t"," ");
			
			//varaibales to store needed info
			String messageID, from, to;
			messageID = from = to = "";
			List<String> ccArray = new ArrayList<String>();
			List<String> bccArray = new ArrayList<String>();
			
			//set message id
			messageID = getMessageID(line);
			uniformOutput.set(messageID);

			//use functions to get needed field from original string			
			from = getFrom(line);
			to = getTo(line);
			ccArray = getCc(line);
			bccArray = getBcc(line);

			//write all the key/value pairs
			context.write(new Text(from),uniformOutput);
			context.write(new Text(to),uniformOutput);
			for (int i = 0;i < ccArray.size();i++){
				context.write(new Text("Cc:"+ccArray.get(i)),uniformOutput);
			}
			for (int j = 0;j < bccArray.size();j++){
				context.write(new Text("Bcc:"+bccArray.get(j)),uniformOutput);
			}
	    }

	    
    	//function to get message id from string, return message id
    	public static String getMessageID(String in){
			String messageIdPattern = "Message-ID:(.+?)Date:";
			String input = in.toString();
			Pattern r = Pattern.compile(messageIdPattern);
			Matcher m = r.matcher(input);
			String output = null;
			if(m.find()){
			    output = m.group(0).toString().replace("Message-ID:","").replace("Date:","").replace(" ","");
			}
			return output;
		  }

		//function to get "from" email address, used to create "From:email" key
		public static String getFrom(String in){
			String fromPattern = "From:(.+?)To:";
			String input = in.toString();
			Pattern r = Pattern.compile(fromPattern);
			Matcher m = r.matcher(input);
			String output = null;
			if(m.find()){
			    output = m.group(0).toString().replace("From:","").replace("To:","").replace(" ","");
			}
			return "From:"+output;
		}

		//function to get "to" email address, used to create "To:email" key
		public static String getTo(String in){
			String toPattern = "To:(.+?)Subject:";
			String input = in.toString();
			Pattern r = Pattern.compile(toPattern);
			Matcher m = r.matcher(input);
			String output = null;
			if(m.find()){
			    output = m.group(0).toString().replace("To:","").replace("Subject:","").replace(" ","");
			}
			return "To:"+output;
		 }

		 //function to get "Cc" email address, used to create "Cc:email" key. Return a list of keys
		public static List<String> getCc(String in){
			String ccPattern = "Cc:(.+?)Mime-Version:";
			String input = in.toString();
			Pattern r = Pattern.compile(ccPattern);
			Matcher m = r.matcher(input);
			String output = null;
			List<String> array = new ArrayList<String>();
			if(m.find()){
			    output = m.group(0).toString().replace("Cc:","").replace("Mime-Version:","").replace(" ","");
			    StringTokenizer tokenizer = new StringTokenizer(output,",");
			    while (tokenizer.hasMoreTokens()) {
			    array.add(tokenizer.nextToken());
			}
			}
			return array;
		}    

		//function to get "Bcc email address, used to create "Bcc:email" key. Return a list of keys
		public static List<String> getBcc(String in){
			String bccPattern = "Bcc:(.+?)X-From:";
			String input = in.toString();
			Pattern r = Pattern.compile(bccPattern);
			Matcher m = r.matcher(input);
			String output = null;
			List<String> array = new ArrayList<String>();
			if(m.find()){
				System.out.println(m.group(0));
			    output = m.group(0).toString().replace("Bcc:","").replace("X-From:","").replace(" ","");
			    StringTokenizer tokenizer = new StringTokenizer(output,",");
			    while (tokenizer.hasMoreTokens()) {
			    array.add(tokenizer.nextToken());
			}
			}
			return array;
		}
	}	
	/**
	 * The Reduce class for Inverted Index.  Extends class Reducer, provided by Hadoop.
	 * This class defines the reduce() function
	 */
	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
            String output = "";
			// Loop to creat reduced result. Iterate a list of Text, write everyone out
			for(Text value:values){
				//if the incoming record is the first one, no need to add ","
				if(output.equals("")){
					output += value.toString();
				}
				//if the incomming value is not the first one, add a ","
				else{
					output += ","+value.toString();
				}
			}
			// Emit the result.
			context.write(key, new Text(output));
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

		Job job = Job.getInstance(conf, "InvertedIndex");
		// Identify the JAR file to replicate to all machines.
		job.setJarByClass(InvertedIndex.class);

		// Set the output key and value types (for map and reduce).
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// Set the map and reduce classes.
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
        job.setCombinerClass(ReduceClass.class);

		// Set the input and output file formats.
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		// Grab the input file and output directory from the command line.
		FileInputFormat.addInputPaths(job, appArgs[0]);
		FileOutputFormat.setOutputPath(job, new Path(appArgs[1]));

		// Initiate the map-reduce job, and wait for completion.
		job.waitForCompletion(true);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		Utils.printClassPath();
		System.setProperty("hadoop.home.dir", "/");
		int res = ToolRunner.run(new InvertedIndex(), args);
		System.exit(res);
	}

}
