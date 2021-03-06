Assignment 10 - Inverted Index in Spark

Due Tuesday by 9:30am  Points 15  Submitting a file upload
For assignment 10, implement an inverted index as you did for Assignment 3, this time using:

Different input data (described below)
Spark instead of Hadoop map-reduce.
The input file is a text file or verses, each line start with a verse ID followed by the verse text. For this assignment you will need to extract the verse ID at the beginning of each line. Format of the ID is:

 book:chapter:verse
Where book is a string, and chapter and verse are integers. The content of the verse is the remainder of the input line (separated from the ID by whitespace). There will be blank lines in the input, so be prepared to handle that in your mapper.

Your output should be in the following format (each line):

the word
tab character
verse references (book:chapter:verse), separated by commas
The output file should be ordered by the words being indexed (lexicographical order), and the verse references should be ordered by book, chapter, and verse number. Remove punctuation and make all the words lower case.

The input files can be found on Canvas: Files / Assignment 10

dataSet10Small.txt - for development
dataSet10.txt - for assignment submission 
Artifacts to submit for the assignment:

Assignment10Build.tar - all files (Java or your source language, pom.xml) in the directory structure required by maven and buildable with your pom.xml file.
Assignment10Code.zip - all files (Java or your source language (Python, Scala)) in a flat directory for easy inspection for grading
Assignment10Output.txt - output generated by your program
 

Extra credit (3 pts.):

In addition to the file output for the base assignment, output another file where the lines are ordered by the number of references in the index, and where this number is the same, order by the word being indexed.