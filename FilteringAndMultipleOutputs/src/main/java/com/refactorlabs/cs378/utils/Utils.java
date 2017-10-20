package com.refactorlabs.cs378.utils;

import org.apache.hadoop.io.LongWritable;

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
	public static final String LARGE_SESSION_COUNT = "Discarded Large Session Counts";
	public static final String CLICK_SESSION_COUNT = "Discarded Clicker Session Counts";
	public static final String SHOWER_SESSION_COUNT = "Discarded Shower Session Counts";

	public static final long ONE = 1L;
	public static final long THREE = 3L;
	public final static LongWritable WRITABLE_ONE = new LongWritable(ONE);
	public final static LongWritable WRITABLE_THREE = new LongWritable(THREE);


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
