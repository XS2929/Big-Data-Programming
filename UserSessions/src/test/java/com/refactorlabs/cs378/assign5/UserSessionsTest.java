package com.refactorlabs.cs378.assign5;

import com.google.common.collect.Lists;
import com.refactorlabs.cs378.utils.Utils;
import junit.framework.Assert;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.Text;
import org.apache.avro.Schema;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;

import org.junit.Before;
import org.junit.Test;
import java.util.*;


import java.io.IOException;

/**
 * Unit test of WordStatisticsA.
 *
 * Demonstrates how AvroSerialization is configured so that MRUnit code
 * understands how to serialize the mapper output and the expected output
 * for comparison.
 *
 * Configuring the AvroSerialization solution was found here:
 * http://stackoverflow.com/questions/15230482/mrunit-with-avro-nullpointerexception-in-serialization
 *
 * Author Xujian Shao (xs2929@cs.utexas.edu)
 */
public class UserSessionsTest {

	MapDriver<LongWritable, Text, Text, AvroValue<Session>> mapDriver;
	ReduceDriver<Text, AvroValue<Session>, AvroKey<CharSequence>, AvroValue<Session>> reduceDriver;

	@Before
	public void setup() {
		System.setProperty("hadoop.home.dir", "/");
		UserSessionsMapper mapper = new UserSessionsMapper();		
		UserSessions.ReduceClass reducer = new UserSessions.ReduceClass();

		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);


		// Copy over the default io.serializations. If you don't do this then you will
		// not be able to deserialize the inputs to the mapper
		String[] strings = mapDriver.getConfiguration().getStrings("io.serializations");
		String[] newStrings = new String[strings.length +1];
		System.arraycopy( strings, 0, newStrings, 0, strings.length );
		newStrings[newStrings.length-1] = AvroSerialization.class.getName();

		// Now you have to configure AvroSerialization by specifying the value writer schema.
		mapDriver.getConfiguration().setStrings("io.serializations", newStrings);
		mapDriver.getConfiguration().setStrings("avro.serialization.key.writer.schema", Schema.create(Schema.Type.LONG).toString(true));
		mapDriver.getConfiguration().setStrings("avro.serialization.value.writer.schema",Session.SCHEMA$.toString(true));

		String[] strings1 = reduceDriver.getConfiguration().getStrings("io.serializations");
		String[] newStrings1 = new String[strings1.length +1];
		System.arraycopy( strings1, 0, newStrings1, 0, strings1.length );
		newStrings1[newStrings1.length-1] = AvroSerialization.class.getName();

		// Now you have to configure AvroSerialization by specifying the value writer schema.
		reduceDriver.getConfiguration().setStrings("io.serializations", newStrings1);
		reduceDriver.getConfiguration().setStrings("avro.serialization.key.writer.schema",Schema.create(Schema.Type.STRING).toString(true));		
		reduceDriver.getConfiguration().setStrings("avro.serialization.value.writer.schema",Session.SCHEMA$.toString(true));

		// If the mapper outputs an AvroKey,
		// we need to configure AvroSerialization by specifying the key writer schema.
//		mapDriver.getConfiguration().setStrings("avro.serialization.key.writer.schema",
//				Schema.create(Schema.Type.STRING).toString(true));
	}

	private static final String TEST_WORD = "541618450\tdisplay alternative\t2017-09-25 06:27:17.000000\tCumming\tSHSRD78985U330364\tUsed\t2005\tHonda\tCR-V\tnull\tSUV\tnull\t8495.0000\t126200\tf\tnull";
    //"541618450\tdisplay alternative\t2017-09-25 06:27:17.000000\tCumming\tSHSRD78985U330364\tUsed\t2005\tHonda\tCR-V\tSE\tSUV\tnull\t8495.0000\t126200\tf\t3-Point Seat Belts:16 Inch Wheels:6 Speakers:AM/FM:Adjustable Headrests:Adjustable Seats:Adjustable Steering Wheel";

	@Test
	public void testMapClass() {
		// Create the expected output value.
		Session.Builder builder = Session.newBuilder();
		Event.Builder eventBuilder = Event.newBuilder();
	   
		eventBuilder.setEventType(EventType.valueOf("DISPLAY"));
		eventBuilder.setEventSubtype(EventSubtype.valueOf("ALTERNATIVE"));
		eventBuilder.setEventTime("2017-09-25 06:27:17.000000");
		eventBuilder.setCity("Cumming");
		eventBuilder.setVin("SHSRD78985U330364");
		eventBuilder.setCondition(Condition.valueOf("USED"));
		eventBuilder.setYear(Integer.valueOf("2005"));
		eventBuilder.setMake("Honda");
		eventBuilder.setModel("CR-V");
		
		//eventBuilder.setTrim("SE");
		eventBuilder.setTrim(null);
		eventBuilder.setBodyStyle(BodyStyle.valueOf("SUV"));
		//eventBuilder.setCabStyle(CabStyle.valueOf("NULL"));
		eventBuilder.setCabStyle(null);

		eventBuilder.setPrice(Double.valueOf("8495.0000"));
		eventBuilder.setMileage(Integer.valueOf("126200")); 
		eventBuilder.setFreeCarfaxReport(false);

		String[] features = {"16 Inch Wheels","3-Point Seat Belts","6 Speakers","AM/FM","Adjustable Headrests", "Adjustable Seats","Adjustable Steering Wheel"};
		List<CharSequence>  featureList = new ArrayList<CharSequence>(); 
		List<CharSequence>  featureList2 = new ArrayList<CharSequence>(); 

        for (int i = 0; i < features.length; i++ ){
            CharSequence toAdd = features[i];
            featureList.add(toAdd);
        }

		eventBuilder.setFeatures(featureList2);


		builder.setUserId("541618450");
		List<Event> eventList = new ArrayList<Event>();
        eventList.add(eventBuilder.build());

        builder.setEvents(eventList);


		mapDriver.withInput(new LongWritable(0L), new Text(TEST_WORD));
		mapDriver.withOutput(new Text("541618450"), new AvroValue<Session>(builder.build()));
		try {
			mapDriver.runTest();
		} catch (IOException ioe) {
			Assert.fail("IOException from mapper: " + ioe.getMessage());
		}
	}

	@Test
	public void testReduceClass() {

		Session.Builder builder = Session.newBuilder();
		Session.Builder builder1 = Session.newBuilder();
		Session.Builder builder2 = Session.newBuilder();
		Session.Builder builder3 = Session.newBuilder();

		Event.Builder eventBuilder3 = Event.newBuilder();
		Event.Builder eventBuilder1 = Event.newBuilder();
		Event.Builder eventBuilder2 = Event.newBuilder();


	    eventBuilder1.setEventType(EventType.valueOf("VISIT"));
		eventBuilder1.setEventSubtype(EventSubtype.valueOf("BADGES"));
		eventBuilder1.setEventTime("2017-09-25 06:27:21.000000");
		eventBuilder1.setCity("Cumming");
		eventBuilder1.setVin("SHSRD78985U330364");
		eventBuilder1.setCondition(Condition.valueOf("USED"));
		eventBuilder1.setYear(Integer.valueOf("2005"));
		eventBuilder1.setMake("Honda");
		eventBuilder1.setModel("CR-V");
		eventBuilder1.setTrim("SE");
		eventBuilder1.setBodyStyle(BodyStyle.valueOf("SUV"));
		eventBuilder1.setCabStyle(CabStyle.valueOf("NULL"));
		eventBuilder1.setPrice(Double.valueOf("8495.0000"));
		eventBuilder1.setMileage(Integer.valueOf("126200")); 
		eventBuilder1.setFreeCarfaxReport(false);


		eventBuilder2.setEventType(EventType.valueOf("DISPLAY"));
		eventBuilder2.setEventSubtype(EventSubtype.valueOf("ALTERNATIVE"));
		eventBuilder2.setEventTime("2017-09-25 06:27:21.000000");
		eventBuilder2.setCity("Cumming");
		eventBuilder2.setVin("SHSRD78985U330364");
		eventBuilder2.setCondition(Condition.valueOf("USED"));
		eventBuilder2.setYear(Integer.valueOf("2005"));
		eventBuilder2.setMake("Honda");
		eventBuilder2.setModel("CR-V");
		eventBuilder2.setTrim("SE");
		eventBuilder2.setBodyStyle(BodyStyle.valueOf("SUV"));
		eventBuilder2.setCabStyle(CabStyle.valueOf("NULL"));
		eventBuilder2.setPrice(Double.valueOf("8495.0000"));
		eventBuilder2.setMileage(Integer.valueOf("126200")); 
		eventBuilder2.setFreeCarfaxReport(true);

		eventBuilder3.setEventType(EventType.valueOf("DISPLAY"));
		eventBuilder3.setEventSubtype(EventSubtype.valueOf("ALTERNATIVE"));
		eventBuilder3.setEventTime("2017-09-21 06:27:21.000000");
		eventBuilder3.setCity("Cumming");
		eventBuilder3.setVin("SHSRD78985U330364");
		eventBuilder3.setCondition(Condition.valueOf("NEW"));
		eventBuilder3.setYear(Integer.valueOf("2005"));
		eventBuilder3.setMake("Honda");
		eventBuilder3.setModel("CR-V");
		eventBuilder3.setTrim("SE");
		eventBuilder3.setBodyStyle(BodyStyle.valueOf("SUV"));
		eventBuilder3.setCabStyle(CabStyle.valueOf("NULL"));
		eventBuilder3.setPrice(Double.valueOf("8495.0000"));
		eventBuilder3.setMileage(Integer.valueOf("126200")); 
		eventBuilder3.setFreeCarfaxReport(true);

		String[] features = {"16 Inch Wheels","3-Point Seat Belts","6 Speakers","AM/FM","Adjustable Headrests", "Adjustable Seats","Adjustable Steering Wheel"};
		List<CharSequence>  featureList = new ArrayList<CharSequence>(); 
        for (int i = 0; i < features.length; i++ ){
            CharSequence toAdd = features[i];
            featureList.add(toAdd);
        }

		eventBuilder3.setFeatures(featureList);
		eventBuilder1.setFeatures(featureList);
		eventBuilder2.setFeatures(featureList);


		builder.setUserId("541618450");
		builder1.setUserId("541618450");
		builder2.setUserId("541618450");
		builder3.setUserId("541618450");

		List<Event> eventList = new ArrayList<Event>();
        eventList.add(eventBuilder3.build());
        eventList.add(eventBuilder2.build());
        eventList.add(eventBuilder1.build());


		List<Event> eventList1 = new ArrayList<Event>();
        eventList1.add(eventBuilder1.build());

        List<Event> eventList2 = new ArrayList<Event>();
        eventList2.add(eventBuilder2.build());

        List<Event> eventList3 = new ArrayList<Event>();
        eventList3.add(eventBuilder3.build());

        builder1.setEvents(eventList1);
        builder2.setEvents(eventList2);
        builder3.setEvents(eventList3);

        builder.setEvents(eventList);





        builder.setEvents(eventList);

		List<AvroValue<Session>> sessionList = Lists.newArrayList(new AvroValue<Session>(builder1.build()),new AvroValue<Session>(builder1.build()),new AvroValue<Session>(builder2.build()),new AvroValue<Session>(builder3.build()));
		//List<AvroValue<WordStatisticsData>> valueList = Lists.newArrayList(sample,sample1);



		reduceDriver.withInput(new Text("541618450"), sessionList);

		reduceDriver.withOutput(new AvroKey<CharSequence>("541618450"), new AvroValue<Session>(builder.build()));
		try {
			reduceDriver.runTest();
		} catch (IOException ioe) {
			Assert.fail("IOException from mapper: " + ioe.getMessage());
		}
	}

}


