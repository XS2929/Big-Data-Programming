package com.refactorlabs.cs378.assign10;

import com.google.common.collect.Lists;
import com.refactorlabs.cs378.utils.Utils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Iterator;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Collections;
import java.util.ArrayList;

/**
 * InvertedIndexInSpark application for Spark.
 */
public class InvertedIndexInSpark {

    //comparator to sort values 
    public static class verseComparator implements Comparator<String>{
        public int compare(String s1, String s2){

            String[] record1 = s1.split(":");
            String[] record2 = s2.split(":");
            //compare first field
            if(!record1[0].equals(record2[0]))
                return record1[0].compareTo(record2[0]);

            if(!record1[1].equals(record2[1])){
                if(Integer.parseInt(record1[1]) > Integer.parseInt(record2[1]))
                    return 1;
                else
                    return -1;
                }
            //compare third field
            if(Integer.parseInt(record1[2]) > Integer.parseInt(record2[2]))
                return 1;
            else if(Integer.parseInt(record1[2]) < Integer.parseInt(record2[2]))
                return -1;
            else 
                return 0;
        }
    }


	public static void main(String[] args) {

		String inputFilename = args[0];
		String outputFilename = args[1];

		// Create a Java Spark context
		SparkConf conf = new SparkConf().setAppName(InvertedIndexInSpark.class.getName()).setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);

		try {

            JavaRDD<String> input = sc.textFile(inputFilename);

            // Split the input into unique words
            FlatMapFunction<String, String> splitFunction =
                    new FlatMapFunction<String, String>() {
                        @Override
                        public Iterator<String> call(String line) throws Exception {
                            List<String> wordList = Lists.newArrayList();
                            //check blank line first
                            if(!line.replace("\n","").equals("")){
                                //use set to guarantee uniqueness of word
                                HashSet<String> uniqueWordSet = new HashSet<String>(); 
                                String temp = "";
                                //split verse index and content
                                String verse = line.substring(0, line.indexOf(' '));
                                String content = line.substring(line.indexOf(' ') + 1);
                                StringTokenizer tokenizer = new StringTokenizer(content.toLowerCase()," .!?()\',_:;\"");
                                //use tokenizer to remove punctuations
                                while (tokenizer.hasMoreTokens()) {
                                    temp = tokenizer.nextToken();
                                    if (!uniqueWordSet.contains(temp))
                                        uniqueWordSet.add(temp);
                                }
                                for (String s : uniqueWordSet) 
                                    wordList.add(s+' '+verse);
                            }
                            return wordList.iterator();

                        }
                    };

            // create Pairs
            PairFunction<String, String, String> createPairFunction =
                    new PairFunction<String, String, String>() {
                        @Override
                        public Tuple2<String, String> call(String s) throws Exception {
                            String[] newPair = s.split(" ");
                            return new Tuple2(newPair[0], newPair[1]);
                        }
                    };

            // create single key value
            Function2<String, String, String> sumValueFunction =
                    new Function2<String, String, String>() {
                        @Override
                        public String call(String s1, String s2) throws Exception {
                            return s1 + " " + s2;
                        }
                    };
            // function to sort values in a record
            Function<String, Iterable<String>> sortValuesFunction = 
                    new Function<String, Iterable<String>>() {
                    public Iterable<String> call(String value) throws Exception {
                        
                        String[] values = value.split(" ");
                        //List<String> valueList = Lists.newArrayList(); 
                        ArrayList<String> valueList = new ArrayList<>();
                        for(int i = 0; i < values.length; i++)
                            valueList.add(values[i]);
                        //use collection to 
                        Collections.sort(valueList, new verseComparator());
                        ArrayList<String> output = new ArrayList<>();
                        output.add(valueList.toString());
                        return output;
                }
            };
            JavaRDD<String> words = input.flatMap(splitFunction);
            JavaPairRDD<String, String> mapOutput = words.mapToPair(createPairFunction);
            JavaPairRDD<String, String> reduceOutput = mapOutput.reduceByKey(sumValueFunction);
            JavaPairRDD<String, String> orderKeyOutput = reduceOutput.sortByKey();
            JavaPairRDD<String, String> orderValueOutput =  orderKeyOutput.flatMapValues(sortValuesFunction); 
            orderValueOutput.saveAsTextFile(outputFilename);
        } finally {
            // Shut down the context
            sc.stop();
        }
	}
}
