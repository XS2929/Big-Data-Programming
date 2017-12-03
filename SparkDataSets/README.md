Assignment 12 - Spark DataSets
 Submit Assignment
Due Thursday by 9:30am  Points 20  
For this assignment, input the file dataSet12.csv into a DataSet and compute the following using SQL:

By make/model, the min, max, and average price (excluding VINs with price = 0)
Order the output CSV by make, then model
By year, the min, max, and average mileage (excluding VINs with mileage - 0)
Order the output CSV by year
By VIN, the total for each event type/action (you'll need to split out the event type/action from the event)
Order the output CSV by VIN, then event type/action
Note that the same VIN (and associated make, model, price, and mileage) appears multiple times in the events, so only consider each VIN once in the price and mileage statistics

Artifacts to submit for the assignment:

Assignment12Build.zip or tar - all files (Java or your language, pom.xml) in the directory structure required by maven and buildable with your pom.xml file.
Assignment12Code.zip - all files (Java or your language) in a flat directory for easy inspection for grading
Assignment12OutputPrice.txt - output for price statistics
Assignment12OutputMileage.txt - output for mileage statistics
Assignment12OutputEvent.txt - output for event statistics