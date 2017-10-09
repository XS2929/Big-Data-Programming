package com.refactorlabs.cs378.assign1;

import com.google.common.collect.Lists;
import com.refactorlabs.cs378.utils.Utils;
import junit.framework.Assert;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * Unit test for the WordCount map-reduce program.
 *
 * @author David Franke (dfranke@cs.utexas.edu)
 */
public class WordCountTest {

	MapDriver<LongWritable, Text, Text, LongWritable> mapDriver;
	ReduceDriver<Text, LongWritable, Text, LongWritable> reduceDriver;

	@Before
	public void setup() {
		System.setProperty("hadoop.home.dir", "/");
		WordCount.MapClass mapper = new WordCount.MapClass();
		WordCount.ReduceClass reducer = new WordCount.ReduceClass();

		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	private static final String TEST_WORD = "541618450	display alternative	2017-09-25 06:27:17.000000	Cumming	SHSRD78985U330364	Used	2005	Honda	CR-V	SE	SUV	null	8495.0000	126200	f	Stability Control:Tachometer:Thermometer";

	@Test
	public void testMapClass() {
		List<LongWritable> valueList = Lists.newArrayList(Utils.WRITABLE_ONE, Utils.WRITABLE_THREE);
		mapDriver.withInput(new LongWritable(0L), new Text(TEST_WORD));
		mapDriver.withOutput(new Text("event_type:display alternative"), Utils.WRITABLE_ONE);
		mapDriver.withOutput(new Text("city:Cumming"), Utils.WRITABLE_ONE);
		mapDriver.withOutput(new Text("vehicle_condition:Used"), Utils.WRITABLE_ONE);
		mapDriver.withOutput(new Text("year:2005"), Utils.WRITABLE_ONE);
		mapDriver.withOutput(new Text("make:Honda"), Utils.WRITABLE_ONE);
		mapDriver.withOutput(new Text("model:CR-V"), Utils.WRITABLE_ONE);
		mapDriver.withOutput(new Text("trim:SE"), Utils.WRITABLE_ONE);
		mapDriver.withOutput(new Text("body_style:SUV"), Utils.WRITABLE_ONE);
		mapDriver.withOutput(new Text("cab_style:null"), Utils.WRITABLE_ONE);
		mapDriver.withOutput(new Text("free_carfax_report:f"), Utils.WRITABLE_ONE);
		mapDriver.withOutput(new Text("feature:0 Stability Control"), Utils.WRITABLE_ONE);
		mapDriver.withOutput(new Text("feature:1 Tachometer"), Utils.WRITABLE_ONE);
		mapDriver.withOutput(new Text("feature:2 Thermometer"), Utils.WRITABLE_ONE);

		try {
			mapDriver.runTest();
		} catch (IOException ioe) {
			Assert.fail("IOException from mapper: " + ioe.getMessage());
		}
	}

	@Test
	public void testReduceClass() {
		List<LongWritable> valueList = Lists.newArrayList(Utils.WRITABLE_ONE, Utils.WRITABLE_ONE, Utils.WRITABLE_ONE);
		reduceDriver.withInput(new Text("event_type:display alternative"), valueList);
		reduceDriver.withOutput(new Text("event_type:display alternative"), new LongWritable(3L));
		try {
			reduceDriver.runTest();
		} catch (IOException ioe) {
			Assert.fail("IOException from mapper: " + ioe.getMessage());
		}
	}
}