package com.refactorlabs.cs378.assign11;

import com.google.common.collect.Lists;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.Accumulator;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Set;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Collections;
import java.util.ArrayList;
import java.sql.Time;

/**
 * UserSessionsInSpark application for Spark.
 */
public class UserSessionsInSpark {
    //Event object class
    private static class Event implements Serializable { 

        String eventType;
        String eventSubType;
        String eventTimestamp;
        String vin; 
        public String toString() { return "<" + eventType + ":" + eventSubType + "," + eventTimestamp + "," + vin + ">";} 
    }
    //comparator to sort events
    public static class EventComparator implements Comparator<Event>, Serializable {

        @Override
        public int compare(Event e1, Event e2){

            java.sql.Timestamp time1 = java.sql.Timestamp.valueOf(e1.eventTimestamp);
            java.sql.Timestamp time2 = java.sql.Timestamp.valueOf(e2.eventTimestamp);
            long thisTime = time1.getTime();
            long thatTime = time2.getTime();

            //compare Timestamp field
            if(thisTime > thatTime)
                return 1;
            else if(thisTime < thatTime)
                return -1;
            //if time is the same, compare types
            if(!e1.eventType.equals(e1.eventType))
                return e1.eventType.compareTo(e1.eventType);
            else
                return e1.eventSubType.compareTo(e1.eventSubType);
        }
    }
    //comparator to sort sessions
    public static class SessionComparator implements Comparator<Tuple2<String, String>>, Serializable {

        @Override
        public int compare(Tuple2<String, String> one, Tuple2<String, String> two){
            //compare id then city
            if(!one._1.equals(two._1))
                return one._1.compareTo(two._1);
            else
                return one._2.compareTo(two._2);
        }
    }
    //Partitioner to partition output
    public static class CustomPartitions extends Partitioner{     
        
        private int numParts = 6; 
        @Override
        public int getPartition(Object key) {
            String city = ((Tuple2)key)._2().toString();
            int hash = city.hashCode();
            if(hash < 0)
                hash *= -1; 
            return hash % numPartitions();
        }
        @Override
        public int numPartitions() {return this.numParts; }              
    }


	public static void main(String[] args) {

        String inputFilenames = args[0];
		String outputFilename = args[1];

		// Create a Java Spark context
		SparkConf conf = new SparkConf().setAppName(UserSessionsInSpark.class.getName()).setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
        //create accumulators
        Accumulator<Integer> eventBeforeSessionFiltering = sc.accumulator(0);
        Accumulator<Integer> totalShowerCountAfterFiltering = sc.accumulator(0);
        Accumulator<Integer> eventAfterSessionFiltering = sc.accumulator(0);
        Accumulator<Integer> totalSessionCount = sc.accumulator(0);
        Accumulator<Integer> totalShowerCount = sc.accumulator(0);

        HashSet<Tuple2<String, String>> sessionSet = new HashSet<Tuple2<String, String>>();
        HashSet<String> uniqueEventSet = new HashSet<String>();
        HashSet<String> uniqueFilteredEventSet = new HashSet<String>();


		try {

            // Split the input line, create key value pair
            PairFunction<String, Tuple2<String,String>, Iterable<Event>> mapFunction =
                    new PairFunction<String, Tuple2<String,String>, Iterable<Event>>() {
                        
                        @Override
                        public Tuple2<Tuple2<String,String>, Iterable<Event>> call(String line) throws Exception {
                            //refine input string to avoid further bugs
                            String newLine = line.toString().replace("\t\t","\tnull\t");
                            String[] lineArray = newLine.split("\t");
                            //set key
                            String userId = lineArray[0];
                            String city = lineArray[3];
                            Tuple2<String,String> outputKey = new Tuple2<String, String>(userId, city);
                            //set event
                            Event newEvent = new Event();
                            newEvent.eventTimestamp = lineArray[2];
                            newEvent.vin = lineArray[4];
                            String[] types = lineArray[1].split(" ");
                            newEvent.eventType = types[0];
                            newEvent.eventSubType = lineArray[1].substring(lineArray[1].indexOf(" ")+1);
                            //create hash string to check uniqueness
                            String checkUnique = userId + city + newEvent.toString();
                            //if event unique, add it to unique set and increment Accumulator
                            //write out value
                            if(!uniqueEventSet.contains(checkUnique)){
                                uniqueEventSet.add(checkUnique);
                                eventBeforeSessionFiltering.add(1);
                                ArrayList<Event> value = new ArrayList<Event>();
                                value.add(newEvent);
                                return new Tuple2<Tuple2<String,String>, Iterable<Event>>(outputKey, value);
                            }
                            //if not unique event, return an empty value 
                            return new Tuple2<Tuple2<String,String>, Iterable<Event>>(outputKey, new ArrayList<Event>());
                        }
            };

            // function to reduce and sort events in session
            Function2<Iterable<Event>, Iterable<Event>, Iterable<Event>> reduceFunction = 
                    new Function2<Iterable<Event>, Iterable<Event>, Iterable<Event>>() {

                    @Override
                    public Iterable<Event> call(Iterable<Event> e1, Iterable<Event> e2) throws Exception {
 
                        ArrayList<Event> allEvents = new ArrayList<Event>();
                        allEvents.addAll((ArrayList<Event>)e1);
                        allEvents.addAll((ArrayList<Event>)e2);
                        HashSet<Event> setList = new HashSet<Event>(allEvents);
                        allEvents.clear();
                        allEvents.addAll(setList);
                        Collections.sort(allEvents, new EventComparator());
                        return allEvents;
                }
            };
            // function to filter out sessions
            Function<Tuple2<Tuple2<String, String>, Iterable<Event>>, Boolean> filterFunction =
                new Function<Tuple2<Tuple2<String, String>, Iterable<Event>>, Boolean> (){
            
            @Override
            public Boolean call(Tuple2<Tuple2<String, String>, Iterable<Event>> session) throws Exception {

                boolean SHOWER = false;
                //check wether the session is shower
                ArrayList<Event> events = new ArrayList<Event>();
                events.addAll((ArrayList<Event>)session._2);
                HashSet<Event> setList = new HashSet<Event>(events);
                events.clear();
                events.addAll(setList);
                Random random = new Random();
                for (Event thisEvent: events){   
                    if(thisEvent.eventType.toLowerCase().equals("click")){
                        SHOWER = false;
                        break;
                    }                 
                    if(thisEvent.eventSubType.toLowerCase().equals("contact form")){
                        SHOWER = false;
                        break;
                    } 
                    if((thisEvent.eventType.toLowerCase().equals("show")) || (thisEvent.eventType.toLowerCase().equals("display"))){
                        SHOWER = true;
                    }
                }
                String checkUnique = session._1.toString();
                if(SHOWER){
                    if (random.nextInt(100)+1 < 11) {
                        return true;                               
                    }
                    else {
                        return false;
                    }
                }else{
                    return true;
                }
            }
        };


        JavaRDD<String> input = sc.textFile(inputFilenames);
        JavaPairRDD<Tuple2<String, String>, Iterable<Event>> mapOutput = input.mapToPair(mapFunction);
        JavaPairRDD<Tuple2<String, String>, Iterable<Event>> reduceOutput = mapOutput.reduceByKey(reduceFunction);
        //function to get total session count and total shower count
        reduceOutput.foreach(new VoidFunction<Tuple2<Tuple2<String, String>, Iterable<Event>>>(){ 
            public void call(Tuple2<Tuple2<String, String>, Iterable<Event>> session) {

                totalSessionCount.add(1);
                ArrayList<Event> events = new ArrayList<Event>();
                events.addAll((ArrayList<Event>)session._2);
                boolean SHOWER = false;
                //check wether the session is shower
                for (Event thisEvent: events){   
                    if(thisEvent.eventType.toLowerCase().equals("click")){
                        SHOWER = false;
                        break;
                    }                 
                    if(thisEvent.eventSubType.toLowerCase().equals("contact form")){
                        SHOWER = false;
                        break;
                    } 
                    if((thisEvent.eventType.toLowerCase().equals("show")) || (thisEvent.eventType.toLowerCase().equals("display"))){
                        SHOWER = true;
                    }
                }

                if(SHOWER)
                    totalShowerCount.add(1);
        }});


        JavaPairRDD<Tuple2<String, String>, Iterable<Event>> filterOutput = reduceOutput.filter(filterFunction);
        JavaPairRDD<Tuple2<String, String>, Iterable<Event>> sortKeyOutput = filterOutput.sortByKey(new SessionComparator(),true);
        //function to count total session amount and total event amount after filtering
        sortKeyOutput.foreach(new VoidFunction<Tuple2<Tuple2<String, String>, Iterable<Event>>>(){ 
            public void call(Tuple2<Tuple2<String, String>, Iterable<Event>> session) {

                ArrayList<Event> events = new ArrayList<Event>();
                events.addAll((ArrayList<Event>)session._2);
                eventAfterSessionFiltering.add(events.size());

                boolean SHOWER = false;
                //check wether the session is shower
                for (Event thisEvent: events){   
                    if(thisEvent.eventType.toLowerCase().equals("click")){
                        SHOWER = false;
                        break;
                    }                 
                    if(thisEvent.eventSubType.toLowerCase().equals("contact form")){
                        SHOWER = false;
                        break;
                    } 
                    if((thisEvent.eventType.toLowerCase().equals("show")) || (thisEvent.eventType.toLowerCase().equals("display"))){
                        SHOWER = true;
                    }
                }

                if(SHOWER)
                    totalShowerCountAfterFiltering.add(1);
        }});
        JavaPairRDD<Tuple2<String, String>, Iterable<Event>> partitionOutput = sortKeyOutput.partitionBy(new CustomPartitions());

        partitionOutput.saveAsTextFile(outputFilename);
        } finally {
            System.out.println("Total number of events before session filtering: " + eventBeforeSessionFiltering.value());
            System.out.println("Total number of events after session filtering: " + eventAfterSessionFiltering.value());
            System.out.println("Total number of sessions: " + totalSessionCount.value());
            System.out.println("Total number of sessions of type SHOWER: " + totalShowerCount.value());
            System.out.println("Total number of sessions of type SHOWER that were filtered out: " + (totalShowerCount.value() - totalShowerCountAfterFiltering.value()));
            // Shut down the context
            sc.stop();
        }
	}
}
