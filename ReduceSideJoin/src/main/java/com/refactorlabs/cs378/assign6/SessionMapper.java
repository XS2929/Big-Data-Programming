package com.refactorlabs.cs378.assign6;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.google.common.collect.Lists;
import org.apache.avro.mapred.AvroKey;


import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * Session map class for ReduceSideJoin, read  in Session and write out required values for reducer
 */
public class SessionMapper extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, Text, AvroValue<VinImpressionCounts>> {

    /**
     * Local variable "word" will contain the word identified in the input.
     */
    @Override
    public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
            throws IOException, InterruptedException {

        //initialize builders
        VinImpressionCounts.Builder vinImpressionCountsBuilder = VinImpressionCounts.newBuilder();

        Text word = new Text();

        //get the events that contain VIN info
        List<Event> events= value.datum().getEvents();

        //Map to store VIN info and click status, each VIN has its own click map which will make VinImpressionCounts building easy
        Map<CharSequence, Map<CharSequence, Long>> clickMap = new TreeMap<CharSequence, Map<CharSequence, Long>>();
        
        //Map to store VIN that has edit contact form happening, only the VINs in this map needs EditContact Form value setting
        Map<CharSequence, Long> editMap = new TreeMap<CharSequence, Long>();

        //for loop to get all the info needed for building
        for (Event event : events){
                //get event from event list
                
                //if current event's VIN not in Map, put it and initialize its click map empty 
                if(!clickMap.containsKey(event.getVin()))
                    clickMap.put(event.getVin(),new TreeMap<CharSequence, Long>());
                //if current event's type is Click, check its subtype and put it in this VIN's click map
                if(event.getEventType().toString().equals("CLICK")){
                    if(!clickMap.get(event.getVin()).containsKey(event.getEventSubtype().toString()))
                        clickMap.get(event.getVin()).put(event.getEventSubtype().toString(),1L);                    
                }
                //if current event is edit contact form and this VIN not in EDIT map yet, put it in  
                if(event.getEventType().toString().equals("EDIT") && event.getEventSubtype().toString().equals("CONTACT_FORM")){
                    if(!editMap.containsKey(event.getVin()))
                        editMap.put(event.getVin(),1L);
                }
        }
        //for loop to push all info to builder, Click map has every VIN's record. Looping it is efficient
        for (Map.Entry<CharSequence, Map<CharSequence, Long>> entry : clickMap.entrySet()){
            //set writer key
            word.set(entry.getKey().toString());
            //set unique user
            vinImpressionCountsBuilder.setUniqueUsers(1L);
            //if this vin's click map is not empty, put click map to builder
            if(!entry.getValue().isEmpty()){
                vinImpressionCountsBuilder.setClicks(entry.getValue());
            }
            //if this vin's click map is empty, put an empty map in to avoid potential error might happen in reducer because of default value null
            else{
                vinImpressionCountsBuilder.setClicks(new TreeMap<CharSequence, Long>());
            }
            //set last field setEditContactForm, if this vin is in edit map set it value long 1 
            if(editMap.containsKey(entry.getKey())){
                vinImpressionCountsBuilder.setEditContactForm(1L);
            }
            //all info needed set, write out current VIN
            context.write(word,new AvroValue<VinImpressionCounts>(vinImpressionCountsBuilder.build()));
        }

    }   
   
}
