package com.refactorlabs.cs378.assign2;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class WordStatisticsWritable implements Writable{

    private long paragraphCount;
    private long wordCount;
    private long wordCountSquare;
    private double mean;
    private double variance;

    public WordStatisticsWritable(){
        paragraphCount = 0;
        wordCount = 0;
        wordCountSquare = 0;
        mean = 0;
        variance = 0;
    }

    public WordStatisticsWritable(long paragraphCount, long wordCount, long wordCountSquare, double mean, double variance){
        this.paragraphCount = paragraphCount;
        this.wordCount = wordCount;
        this.wordCountSquare = wordCountSquare;
        this.mean = mean;
        this.variance = variance;
    }

    public void setParagraphCount(long paragraphCount){
        this.paragraphCount = paragraphCount;
    }

    public void setWordCount(long wordCount){
        this.wordCount = wordCount;
    }

    public void setWordCountSquare(long wordCountSquare){
        this.wordCountSquare = wordCountSquare;
    }

    public long getParagraphCount(){
        return paragraphCount;
    }

    public long getWordCount(){
        return wordCount;
    }

    public long getWordCountSquare(){
        return wordCountSquare;
    }


    public void setMean(double mean){
        this.mean = mean;
    }

    public void setVariance(double variance){
        this.variance = variance;
    }

    public double getMean(){
        return mean;
    }

    public double getVariance(){
        return variance;
    }

    @Override
    public boolean equals(Object o){
        if (o instanceof WordStatisticsWritable){
            WordStatisticsWritable tp = (WordStatisticsWritable) o;
            return (paragraphCount == tp.paragraphCount) && (wordCount == tp.wordCount) && (wordCountSquare == tp.wordCountSquare) && (mean == tp.mean) && (variance == tp.variance);
        }

        return false;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        long tempLong = in.readLong();
        setParagraphCount(tempLong);
        tempLong = in.readLong();
        setWordCount(tempLong);
        tempLong = in.readLong();
        setWordCountSquare(tempLong);
        double tempDouble = in.readDouble(); 
        setMean(tempDouble);
        tempDouble = in.readDouble();
        setVariance(tempDouble);
    }
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(getParagraphCount());
        out.writeLong(getWordCount());
        out.writeLong(getWordCountSquare());
        
        out.writeDouble(getMean());
        out.writeDouble(getVariance());
    }

    @Override
    public int hashCode() {
        int result = 17;
        result = 31 * result + Long.hashCode(paragraphCount);
        result = 31 * result + Long.hashCode(wordCount);
        result = 31 * result + Long.hashCode(wordCountSquare);
        result = 31 * result + Double.hashCode(mean);
        result = 31 * result + Double.hashCode(variance);
        return result;
    }
    @Override
    public String toString() {
        return getParagraphCount()+","+getMean()+","+getVariance();
    }



}
