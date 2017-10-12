For this assignment, we'll join two different data sources using a reduce-side join. You'll use MultipleInputs for this assignment, with a mapper defined for each data source.

The first input will be user sessions like you created in assignment 5. First, modify your assignment 5 code to output an AVRO container file (binary) by using the output file format: AvroKeyValueOutputFormat

Output types for your reducer should be:

AvroKey<CharSequence>, AvroValue<Session>

Note: Substitute your session class name for "Session" if you used different name.

The first mapper will read the AVRO container file with your sessions. You'll need to specify AvroKeyValueInputFormat as the input format in order to read this file. Mapper input types must match the output types you used to write the AVRO container file:

AvroKey<CharSequence>, AvroValue<Session>

The second mapper will read a CSV file with the following content and format:

vin,impression_type,count

where VIN is a string like "1J4FA49S41P344436", impression_type is one of these two values (strings), "SRP" or "VDP", and count is an integer representing the number of unique users that viewed the VIN either on a search results page (SRP) or a vehicle detail page (VDP) on car search web sites.

Both mappers should output data using the AVRO schema I've provided (see the file vinImpressionStats.avsc on Canvas). The output types for both mappers will be:

Text, AvroValue<VinImpressionCounts>

The reducer will combine (join) the data from each data source using the VIN as the common key. The join operation should sum the counts from each source (sessions, or vinImpressionStats) and then combine the sums in an instance of the AVRO defined class VinImpressionCounts.

The AVRO object VinImpressionCounts

The first mapper should read your session file and write one instance of VinImpressionCounts for each VIN in the session. The fields should contain the following data:

unique_users - Counts the number of user sessions that reference this VIN.
clicks - For each event of type "CLICK" for this VIN, add the event subtype to this map (with count 1 for the current user - we are counting unique users, not total clicks).
edit_contact_form - For each event type/subtype EDIT/CONTACT_FORM, count 1 for the current user if this event occurred at least once in the session for the VIN. 

The second mapper will take the count field from its input file and record it in the marketplace_srps or marketplace_vdps field. The key output by both mappers is the VIN.

Reduce step

The particular join we are doing is left outer join, where your session data is considered the left input. There might be VINs in the session data (left side) that do not appear in the VIN impression data (right side), and there will be VINs that appear only in the VIN impression data (right side). Left outer join implies that you should output data for all VINs that occur in your session data (left side), but only those VINs from the VIN impression data (right side) that also occur in the session data (left side).

Hints for the run() method of your app:

Define the input paths for your mappers using MultipleInputs.addInputPath(). The input format for the mapper that processes sessions will be AvroKeyValueInputFormat, and the input format for the mapper that process VIN impression counts will be TextInputFormat.
You will also need to add these calls in order to read the AVRO container file: AvroJob.setInputKeySchema(), and AvroJob.setInputValueSchema().
Use TextOutputFormat for reducer output (we want this to be human readable).
Inputs

AVRO container file with your user sessions.
VIN impression counts:
dataSet6VinCounts1.csv
dataSet6VinCounts2.csv
Required elements

Create an AVRO container file for input by modifying your assignment 5 session generator app. (Don't include this file of generated sessions in your assignment submission.)
Define two mappers.
Use MultipleInputs to associate each mapper class with its input file. Note: Since there are two files with VIN impression count data, you'll need a call to addInputFile() for each input file.
Implement left outer join in your reducer, where the data from your user sessions is considered the "left" input.
Artifacts to submit

Assignment6Build.zip or tar - all files (Java, avsc, pom.xml) in the directory structure required by maven and buildable with your pom.xml file.
Assignment6Code.zip - all files (Java, avsc) in a flat directory for easy inspection for grading
Assignment6Output.txt - output file containing the text representation of VinImpressionCounts objects   
