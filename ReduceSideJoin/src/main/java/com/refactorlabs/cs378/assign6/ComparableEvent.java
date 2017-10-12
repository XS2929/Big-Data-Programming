package com.refactorlabs.cs378.assign6;

import java.util.*;
import java.sql.Time;
//the self defined class to sort events
public class ComparableEvent implements Comparable<ComparableEvent>{

	private Event event; 
	
	public ComparableEvent(Event event){
		this.event = event;
	}

	public Event getEvent(){
		return this.event;
	}

	@Override
	public int compareTo(ComparableEvent e){
		ComparableEvent toCompare = (ComparableEvent) e;
		//get the time of the two camparables as string
		String thisTime = getEvent().getEventTime().toString();
		String thatTime = toCompare.getEvent().getEventTime().toString();
		//convert the timestamp to long, for easing comparing
    	java.sql.Timestamp time1 = java.sql.Timestamp.valueOf(thisTime);
    	java.sql.Timestamp time2 = java.sql.Timestamp.valueOf(thatTime);
		long thisTime1 = time1.getTime();
    	long thatTime2 = time2.getTime();
		
		//get the result of time comparing
		//if have same time. compare their event type using string.compareTo() function
		if (thisTime1 > thatTime2)
			return 1;
		else if (thisTime1 < thatTime2)
			return -1;
		else{
			String thisEventType = getEvent().getEventType().toString();
			String thatEventType = toCompare.getEvent().getEventType().toString();
			if(thisEventType.compareTo(thatEventType) > 0 )
				return 1;
			else if(thisEventType.compareTo(thatEventType) < 0)
				return -1;
			else 
				return 0;
		}
	}
}