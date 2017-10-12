package com.refactorlabs.cs378.assign6;

import com.refactorlabs.cs378.utils.Utils;
import org.apache.avro.mapred.AvroValue;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.google.common.collect.Lists;

import java.io.IOException;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/**
 * Map class csv input0
 */
public class CSVMapper extends Mapper<LongWritable, Text, Text, AvroValue<VinImpressionCounts>> {

    /**
     * Local variable "word" will contain the word identified in the input.
     */

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        Text word = new Text();
        VinImpressionCounts.Builder vinImpressionCountsBuilder = VinImpressionCounts.newBuilder();
        //split the input string by comma, index 0 contains VIN code, index 1 contains SRP or VDP, index 2 contains value 
        String[] fields = value.toString().split(",");
        word.set(fields[0]);
        //if index 2 is SRP, set SRP value in VinImpressionCounts. Else set VDP. 
        if(fields[1].equals("SRP")){
            vinImpressionCountsBuilder.setMarketplaceSrps(Long.valueOf(fields[2]));
        }
        else{
            vinImpressionCountsBuilder.setMarketplaceVdps(Long.valueOf(fields[2]));
        }
        //initialize an empty map in clicks, avoiding further errors might be caused by default null
        vinImpressionCountsBuilder.setClicks(new TreeMap<CharSequence, Long>());
        //write
        context.write(word,new AvroValue<VinImpressionCounts>(vinImpressionCountsBuilder.build()));
    }
    
}
