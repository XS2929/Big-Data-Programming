Assignment 11 - User Sessions in Spark

Due Nov 28 by 9:30am  Points 20  Submitting a file upload
For assignment 11 you will create user sessions in Spark, similar to what you did in assignment 5. In particular:

Remove duplicate events
When sorting by timestamp, use the event type as a secondary sort, event subtype as a tertiary sort
Instead of using AVRO to represent a session, you should represent a session in a JavaPairRDD where the key is the tuple <userID, city>, and the value is an array of events, where each event is an instance of the class provided here (create a similar structure if you are using Scala or Python):

private static class Event implements Serializable { 
String eventType;
String eventSubType;
String eventTimestamp;
String vin; 
public String toString() { return "<" + eventType + ":" + eventSubType + "," + eventTimestamp + ">";} 
}
The eventType and eventSubType should be derived from the event_type field from the input, the eventTimestamp should be the event_timestamp field from the input, and the vin should be the vin from the input. The city that you add to the key (along with the userID) should be the city field from the input file.

Data Set

Use dataSet5a.tsv and dataSet5b.tsv (available on Canvas). 

Required elements

Once events have been organized into sessions, do the following

For sessions of type "SHOWER", sample these sessions at a rate of 1 in 10.
A "SHOWER" session is defined as:
The session contains no event whose subtype is Contact Form (not a SUBMITTER session)
The session contains no Click event (not a CLICKER session)
The session contains at least one event of type Show or Display
Order the events in each session by timestamp
Order sessions by userID, then by city.
Partition user sessions by city using a custom partitioner and apply this partitioning at some point, so that the final outputs are partitioned in this way. Your custom partitioner should distribute the sessions by hashing the city, and should produce 6 partitions.
Add accumulators to count:
Total number of events (2 different counts):
After duplicates removed, but before session filtering
After session filtering
Total number of sessions
Total number of sessions of type SHOWER
Total number of sessions of type SHOWER that were filtered out
Output these counts to System.out (with a descriptive string for each, identifying the particular count) and include this output in the artifacts you submit.
 

Artifacts to submit

Assignment11Build.zip or tar - all files (Java or your language, pom.xml) in the directory structure required by maven and buildable with your pom.xml file.
Assignment11Code.zip - all files (Java or your language) in a flat directory for easy inspection for grading
Assignment11Output.tar - tar or zip the output directory, containing all 6 output files and a file with the accumulator count outputs.
