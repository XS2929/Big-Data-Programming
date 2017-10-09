package com.refactorlabs.cs378.assign4;

import com.google.common.collect.Lists;
import com.refactorlabs.cs378.utils.Utils;
import junit.framework.Assert;
import org.apache.avro.hadoop.io.AvroSerialization;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
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
public class AVROTest {

    MapDriver<LongWritable, Text, Text, AvroValue<WordStatisticsData>> mapDriver;
    ReduceDriver<Text, AvroValue<WordStatisticsData>, Text, AvroValue<WordStatisticsData>> reduceDriver;

    @Before
    public void setup() {
        WordStatisticsMapper mapper = new WordStatisticsMapper();
        
        WordStatisticsA.ReduceClass reducer = new WordStatisticsA.ReduceClass();

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
        mapDriver.getConfiguration().setStrings("avro.serialization.value.writer.schema",
                WordStatisticsData.SCHEMA$.toString(true));

        String[] strings1 = reduceDriver.getConfiguration().getStrings("io.serializations");
        String[] newStrings1 = new String[strings1.length +1];
        System.arraycopy( strings1, 0, newStrings1, 0, strings1.length );
        newStrings1[newStrings1.length-1] = AvroSerialization.class.getName();

        // Now you have to configure AvroSerialization by specifying the value writer schema.
        reduceDriver.getConfiguration().setStrings("io.serializations", newStrings1);
        reduceDriver.getConfiguration().setStrings("avro.serialization.value.writer.schema",
                WordStatisticsData.SCHEMA$.toString(true));

        // If the mapper outputs an AvroKey,
        // we need to configure AvroSerialization by specifying the key writer schema.
//		mapDriver.getConfiguration().setStrings("avro.serialization.key.writer.schema",
//				Schema.create(Schema.Type.STRING).toString(true));
    }

    private static final String TEST_WORD = "Yadayada Yadayada Yadayada Yadayada Yadayada Yadayada";

    @Test
    public void testMapClass() {
        // Create the expected output value.
        WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();
        
        builder.setDocumentCount(Utils.ONE);
        builder.setTotalCount(6);
        builder.setMin(0);
        builder.setMax(0);
        builder.setSumOfSquares(36);
        builder.setMean(0);
        builder.setVariance(0);


        mapDriver.withInput(new LongWritable(0L), new Text(TEST_WORD));
        mapDriver.withOutput(new Text("yadayada"), new AvroValue<WordStatisticsData>(builder.build()));
        try {
            mapDriver.runTest();
        } catch (IOException ioe) {
            Assert.fail("IOException from mapper: " + ioe.getMessage());
        }
    }

    @Test
    public void testReduceClass() {
        // Create the expected output value.
        WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();
        
        builder.setDocumentCount(Utils.ONE);
        builder.setTotalCount(6);
        builder.setMin(0);
        builder.setMax(0);
        builder.setSumOfSquares(36);
        builder.setMean(0.0);
        builder.setVariance(0.0);
        AvroValue<WordStatisticsData> sample = new AvroValue<WordStatisticsData>(builder.build());
        builder.setDocumentCount(Utils.ONE);
        builder.setTotalCount(5);
        builder.setMin(0);
        builder.setMax(0);
        builder.setSumOfSquares(25);
        builder.setMean(0.0);
        builder.setVariance(0.0);
        AvroValue<WordStatisticsData> sample1 = new AvroValue<WordStatisticsData>(builder.build());



        List<AvroValue<WordStatisticsData>> valueList = Lists.newArrayList(sample,sample1);
        reduceDriver.withInput(new Text("yadayada"), valueList);

        builder.setDocumentCount(2);
        builder.setTotalCount(11);
        builder.setMin(5);
        builder.setMax(6);
        builder.setSumOfSquares(61);
        builder.setMean(5.5);
        builder.setVariance(0.25);

        reduceDriver.withOutput(new Text("yadayada"), new AvroValue<WordStatisticsData>(builder.build()));
        try {
            reduceDriver.runTest();
        } catch (IOException ioe) {
            Assert.fail("IOException from mapper: " + ioe.getMessage());
        }
    }

}


