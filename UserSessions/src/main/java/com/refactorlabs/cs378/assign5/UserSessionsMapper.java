package com.refactorlabs.cs378.assign5;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * Map class for various WordStatistics that use the AVRO generated class WordStatisticsData.
 */
public class UserSessionsMapper extends Mapper<LongWritable, Text, Text, AvroValue<Session>> {

    /**
     * Local variable "word" will contain the word identified in the input.
     */

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //insert null to missing values for further splitting
        String line = value.toString().replace("\t\t","\tnull\t");
        String[] stringArray = line.split("\t");
        
        //String getEventSubType = "(?<=\\s).*";
        //String getEventType = "^([^\\s]+)"; 

        boolean defaultBool = false;
        //initialize builders
        Session.Builder sessionBuilder = Session.newBuilder();
        Event.Builder eventBuilder = Event.newBuilder();

        Text word = new Text();
        //set output key value
        word.set(stringArray[0]);

        sessionBuilder.setUserId(stringArray[0]);
        //get type and subtype from field
        String[] types = stringArray[1].split(" ",2);
        //Pattern r = Pattern.compile(getEventType);
        //Matcher m = r.matcher(stringArray[1]);
        String eventType = types[0].toString().toUpperCase();

        //Pattern r1 = Pattern.compile(getEventSubType);
        //Matcher m1 = r1.matcher(stringArray[1]);
        String eventSubtype = types[1].toString().replace(" ","_").toUpperCase(); 


        //set values to other fields
        eventBuilder.setEventType(EventType.valueOf(eventType));
        eventBuilder.setEventSubtype(EventSubtype.valueOf(eventSubtype));
        eventBuilder.setEventTime(stringArray[2]);
        eventBuilder.setCity(stringArray[3]);
        eventBuilder.setVin(stringArray[4]);
        eventBuilder.setCondition(Condition.valueOf(stringArray[5].toUpperCase()));
        eventBuilder.setYear(Integer.valueOf(stringArray[6]));
        eventBuilder.setMake(stringArray[7]);
        eventBuilder.setModel(stringArray[8]);
        
        //if Trim is null, set null. Else set a string value
        if (stringArray[9].equals("null"))
            eventBuilder.setTrim(null);
        else
            eventBuilder.setTrim(stringArray[9]);

        eventBuilder.setBodyStyle(BodyStyle.valueOf(stringArray[10].toUpperCase()));
        //only get the first string value for cab style enum
        String[] cabStyleArray = stringArray[11].split(" ");
        //if (cabStyleArray[0].equals("null"))
            //eventBuilder.setCabStyle(CabStyle.valueOf(null));
        //else

        //set null to cab style if input is null, else set a string value
        if (cabStyleArray[0].equals("null"))
            eventBuilder.setCabStyle(null);
        else
            eventBuilder.setCabStyle(CabStyle.valueOf(cabStyleArray[0].toUpperCase()));


        eventBuilder.setPrice(Double.valueOf(stringArray[12]));
        eventBuilder.setMileage(Integer.valueOf(stringArray[13]));
        //get boolean value, set it to builder
        if(stringArray[14].equals("t"))
            defaultBool = true;  
        eventBuilder.setFreeCarfaxReport(defaultBool);
        eventBuilder.setFeatures(getFeatureList(stringArray[15]));
        
        //store the event in event list and put it in builder
        List<Event> eventList = new ArrayList<Event>();
        eventList.add(eventBuilder.build());

        sessionBuilder.setEvents(eventList);

 
        context.write(word,new AvroValue<Session>(sessionBuilder.build()));
    }
    //the function to split features, if null return an empty list, else return a decent list with values
    private List<CharSequence> getFeatureList(String string){

        List<CharSequence>  featureList = new ArrayList<CharSequence>(); 
        if (string.equals("null"))
            return featureList;
        String[] featureArray = string.split(":");
        Arrays.sort(featureArray);

        for (int i = 0; i < featureArray.length; i++ ){
            CharSequence toAdd = featureArray[i];
            featureList.add(toAdd);
        }

        return featureList; 
    }
}
