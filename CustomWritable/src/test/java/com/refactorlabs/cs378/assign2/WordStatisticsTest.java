package com.refactorlabs.cs378.assign2;
import com.google.common.collect.Lists;
import com.refactorlabs.cs378.utils.Utils;
import com.refactorlabs.cs378.assign2.WordStatisticsWritable;
import junit.framework.Assert;
import java.util.logging.Logger;
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
 * @author BULBASAUR
 */
public class WordStatisticsTest {

	MapDriver<LongWritable, Text, Text, WordStatisticsWritable> mapDriver;
	ReduceDriver<Text, WordStatisticsWritable, Text, WordStatisticsWritable> reduceDriver;

	@Before
	public void setup() {
		WordStatistics.MapClass mapper = new WordStatistics.MapClass();
		WordStatistics.ReduceClass reducer = new WordStatistics.ReduceClass();

		mapDriver = MapDriver.newMapDriver(mapper);
		reduceDriver = ReduceDriver.newReduceDriver(reducer);
	}

	private static final String TEST_WORD = "I";

	@Test
	public void testMapClass() {
		mapDriver.withInput(new LongWritable(0L), new Text(TEST_WORD));
		mapDriver.withOutput(new Text("i"), new WordStatisticsWritable(1,1,1,0,0));
		try {
			mapDriver.runTest();
		} catch (IOException ioe) {
			Assert.fail("IOException from mapper: " + ioe.getMessage());
		}
	}

	@Test
	public void testReduceClass() {
		List<WordStatisticsWritable> valueList = Lists.newArrayList(Utils.WRITABLE_ONE, Utils.WRITABLE_THREE);
		reduceDriver.withInput(new Text("yadayada"), valueList);
		reduceDriver.withOutput(new Text("yadayada"), new WordStatisticsWritable(2,4,10,2,1));
		try {
			reduceDriver.runTest();
		} catch (IOException ioe) {
			Assert.fail("IOException from mapper: " + ioe.getMessage());
		}
	}
}
