package com.refactorlabs.cs378.utils;

import org.apache.hadoop.io.LongWritable;
import com.refactorlabs.cs378.assign2.WordStatisticsWritable;

import java.net.URL;
import java.net.URLClassLoader;

/**
 * Created by davidfranke on 10/5/14.
 */
public class Utils {

	// Not instantiable
	private Utils() {}

	/**
	 * Counter groups.  Individual counters are organized into these groups.
	 */
	public static final String MAPPER_COUNTER_GROUP = "Mapper Counts";
	public static final String COMBINER_COUNTER_GROUP = "Combiner Counts";
	public static final String REDUCER_COUNTER_GROUP = "Reducer Counts";

	public static final long LONG_ONE = 1;
    public static final long LONG_THREE = 3;
    public static final long LONG_NINE = 9;
    public static final long LONG_FOUR = 4;
        public static final long LONG_27 = 27;
	public static final double DOUBLE_ONE = 1;
	public static final double DOUBLE_ZERO = 0;
	public static final double DOUBLE_THREE = 3;
	public final static WordStatisticsWritable WRITABLE_ONE = new WordStatisticsWritable(LONG_ONE, LONG_THREE, LONG_NINE, DOUBLE_ZERO, DOUBLE_ZERO);
	public final static WordStatisticsWritable WRITABLE_TWO = new WordStatisticsWritable(LONG_THREE,LONG_NINE,LONG_27,DOUBLE_THREE,DOUBLE_ZERO);
	public final static WordStatisticsWritable WRITABLE_THREE = new WordStatisticsWritable(LONG_ONE, LONG_ONE, LONG_ONE, DOUBLE_ZERO, DOUBLE_ZERO);
	public final static WordStatisticsWritable WRITABLE_FOUR = new WordStatisticsWritable(LONG_ONE, LONG_THREE, LONG_NINE, DOUBLE_ZERO, DOUBLE_ZERO);


    /**
	 * Writes the classpath to standard out, for inspection.
	 */
	public static void printClassPath() {
		ClassLoader cl = ClassLoader.getSystemClassLoader();
		URL[] urls = ((URLClassLoader) cl).getURLs();
		System.out.println("classpath BEGIN");
		for (URL url : urls) {
			System.out.println(url.getFile());
		}
		System.out.println("classpath END");
		System.out.flush();
	}

}
