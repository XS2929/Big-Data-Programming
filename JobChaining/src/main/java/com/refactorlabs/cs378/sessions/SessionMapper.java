package com.refactorlabs.cs378.sessions;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.mapred.AvroValue;
import org.apache.avro.mapreduce.AvroMultipleOutputs;
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
 * Session map class for FilteringAndMultipleOutputs, read  in Session and write out session type. Mapper only job. 
 */
public class SessionMapper extends Mapper<AvroKey<CharSequence>, AvroValue<Session>, AvroKey<CharSequence>, AvroValue<Session>> {

    private AvroMultipleOutputs multipleOutputs;
    /**
     * map method
     */
    @Override
    public void map(AvroKey<CharSequence> key, AvroValue<Session> value, Context context)
            throws IOException, InterruptedException {
        //since there are different levels for a session/user, using numbers to represent the levels is convenient 
        //1, 2, 3, 4, 5, 6 represents submitter, clicker, shower, visitor, other and large session(has more than 100 events)
        //smaller value represent higher level
        //set default level as other
        int userLevel = 5;
        //import random to create integers in interval [1,100] to do random sampling
        Random rand = new Random();
        

        
        List<Event> events= value.datum().getEvents();
        //check large session, if is large session plus one to the large session counter and set level as 6
        if(events.size() > 100){
            context.getCounter(Utils.LARGE_SESSION_COUNT, "Discarded Large Session Counts").increment(1L);
            userLevel = 6;
        }
        //if is not large session, have to iterate through events to determine final level
        else{

            for (Event event : events){
             
                    if(event.getEventSubtype() == EventSubtype.CONTACT_FORM){
                        //if current event is change contact form, edit contact form or submit contact form, set level as 1
                        if(event.getEventType() == EventType.CHANGE || event.getEventType() == EventType.EDIT || event.getEventType() == EventType.SUBMIT)
                            userLevel = 1;
                        //if current event is visit contact form , set level as 4
                        if(event.getEventType() == EventType.VISIT && userLevel > 4)
                            userLevel = 4;                                              
                    }
                    //if event type is click, reset level as 2
                    if((event.getEventType() == EventType.CLICK) && userLevel > 2)
                        userLevel = 2; 
                    //if event type is show or display,reset level as 2
                    if((event.getEventType() == EventType.SHOW || event.getEventType() == EventType.DISPLAY) && userLevel > 3)
                        userLevel = 3; 
                    //if event type is visit, set level as 4
                    if(event.getEventType() == EventType.VISIT && userLevel > 4)
                        userLevel = 4; 
            }        
        }
        //write as submitter directly when final level value is 1
        if (userLevel == 1)
            multipleOutputs.write(SessionType.SUBMITTER.getText(), key, value);
        //when level is 2
        //if random value is less than 11, write out as clicker. If not, count as discarded
        if (userLevel == 2){
                multipleOutputs.write(SessionType.CLICKER.getText(), key, value);
        }
        //when level is 3
        //if random value is less than 3, write out as shower. If not, count as discarded
        if (userLevel == 3){
                multipleOutputs.write(SessionType.SHOWER.getText(), key, value); 
        }
        //write out directly when level is 4
        if (userLevel == 4)
            multipleOutputs.write(SessionType.VISITOR.getText(), key, value);
        //write out directly when level is 5
        if (userLevel == 5)
            multipleOutputs.write(SessionType.OTHER.getText(), key, value);
    }
       

    @Override
    public void setup(Context context) throws IOException{
        multipleOutputs = new AvroMultipleOutputs(context);
    }

    @Override
    public void cleanup(Context context) throws InterruptedException, IOException{
        multipleOutputs.close();
    }    
   







}
