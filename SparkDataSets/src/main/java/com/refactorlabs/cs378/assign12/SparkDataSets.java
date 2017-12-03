package com.refactorlabs.cs378.assign12;
import org.apache.spark.sql.*;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;


public class SparkDataSets {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		//get input and output path
		String inputFilename = args[0];
		String outputFilename = args[1];

		// Create a Java Spark context
		SparkConf conf = new SparkConf().setAppName(SparkDataSets.class.getName()).setMaster("local");
		SparkContext sc = new SparkContext(conf);
        SparkSession sparks = new SparkSession(sc);


		Dataset<Row> info = sparks.read().csv(inputFilename);
		info.registerTempTable("original_table"); 
        //info.repartition(1).write().format("csv").save(outputFilename+"/original_table");

        //part 1
		Dataset<Row> info2 = sparks.sql("SELECT DISTINCT _c3 as vin, _c6 as make, _c7 as model, _c8 as price FROM original_table WHERE CAST(_c8 as DOUBLE) != 0");
		info2.registerTempTable("price_info_table"); 

	    Dataset<Row> info3 = sparks.sql("SELECT make, model, MIN(CAST(price AS DOUBLE)), MAX(CAST(price as DOUBLE)), AVG(CAST(price AS DOUBLE)) FROM price_info_table GROUP BY make, model ORDER BY make, model");
	    info3.registerTempTable("price_statistics_table");
	    info3.repartition(1).write().format("csv").save(outputFilename+"/price_statistics_table");

		//part 2
		Dataset<Row> info4 = sparks.sql("SELECT DISTINCT _c3 as vin, _c5 as year, _c9 as mileage FROM original_table WHERE CAST(_c9 as INT) != 0");
		info4.registerTempTable("mileage_info_table"); 

	    Dataset<Row> info5 = sparks.sql("SELECT year, MIN(CAST(mileage AS INT)), MAX(CAST(mileage as INT)), AVG(CAST(mileage AS DOUBLE)) FROM mileage_info_table GROUP BY year ORDER BY year");
	    info5.registerTempTable("mileage_statistics_table");
	    info5.repartition(1).write().format("csv").save(outputFilename+"/mileage_statistics_table");

	    //part 3
	    Dataset<Row> info6 = sparks.sql("SELECT _c3 as vin, substring_index(CAST(_c1 as STRING),\" \", 1) as event FROM original_table");
		info6.registerTempTable("event_info_table"); 
		//info6.repartition(1).write().format("csv").save(outputFilename+"/event_info_table");
	    
	    Dataset<Row> info7 = sparks.sql("SELECT vin, event, count(event) FROM event_info_table WHERE CAST(vin as STRING) != \"vin\" GROUP BY vin, event ORDER BY vin, event");
	    info7.registerTempTable("event_statistics_table");
	    info7.repartition(1).write().format("csv").save(outputFilename+"/event_statistics_table");

		sc.stop();

	}
}
