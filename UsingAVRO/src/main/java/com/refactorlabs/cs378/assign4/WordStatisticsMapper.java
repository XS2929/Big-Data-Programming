package com.refactorlabs.cs378.assign4;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.*;

/**
 * Map class for various WordStatistics that use the AVRO generated class WordStatisticsData.
 */
public class WordStatisticsMapper extends Mapper<LongWritable, Text, Text, AvroValue<WordStatisticsData>> {

    /**
     * Local variable "word" will contain the word identified in the input.
     */
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString().toLowerCase();
        StringTokenizer tokenizer = new StringTokenizer(line);
        Hashtable<String,Long> words = new Hashtable<String,Long>();
        // For each word in the input line, store the count values into a hashtable.
        while (tokenizer.hasMoreTokens()) {
            String currentWord = tokenizer.nextToken();
            //paragraphLength ++;    
            if(words.containsKey(currentWord)){
                words.put(currentWord,words.get(currentWord)+1L);
            }
            else{
                words.put(currentWord,1L);
            }
        }
        Set<String> keys = words.keySet();
        //loop through thhe hashtable, write out values
        for(String tempKey: keys){
            WordStatisticsData.Builder builder = WordStatisticsData.newBuilder();
            word.set(tempKey);
            builder.setDocumentCount(Utils.ONE);
            builder.setTotalCount(words.get(tempKey));
            builder.setMin(0);
            builder.setMax(0);
            builder.setSumOfSquares(words.get(tempKey)*words.get(tempKey));
            builder.setMean(0.0);
            builder.setVariance(0.0);
 
            context.write(word,new AvroValue<WordStatisticsData>(builder.build()));
        }
    }
}