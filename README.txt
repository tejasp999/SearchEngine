Readme file Instructions:
--All the MapReduce jobs were executed in Cloudera
--The Canterbury input files should be moved from the local file system to the input folder in Hadoop system.

Terminal Command: hadoop fs -put <local file path> <input file path>

Creating Jar File:
//Commented out the package name and made different directories for each execution
--mkdir -p <DirectoryName>
--javac -cp /usr/lib/hadoop/*:/usr/lib/hadoop-mapreduce/* <Filename.java> -d <Directory Name> -Xlint 
--jar -cvf <jarName>.jar -C <DirectoryName>/ .

1. DocWordCount
--After compiling and making jar file, the run command requires two arguments which are input and output paths

Terminal command : hadoop jar <Jar file name> DocWordCount <Hadoop Input file path> <Hadoop Output file path>


2. TermFrequency
--After compiling and making jar file, the run command requires two arguments which are input and output paths

Terminal command : hadoop jar <Jar file name> TermFrequency <Hadoop Input file path> <Hadoop Output file path>

3. TFIDF
--After compiling and making jar file, the run command requires two arguments which are input and output paths

Terminal command : hadoop jar <Jar file name> TFIDF <Hadoop Input file path> <Hadoop Output file path>

Note : In the TFIDF execution, a new folder with the name "TermFreqOutput" is being created in order to give Term Frequency output as input path to calculate TFIDF. In order to run the TFIDF class again, need to delete "TermFreqOutput" folder as well as Output folder in the arguments


4. Search
--After compiling and making jar file, the run command requires three arguments which are input, output paths, query words. In this case, the input becomes the output path of the TFIDF job in the previous step.

Terminal command : hadoop jar <Jar file name> TFIDF <Hadoop Input file path> <Hadoop Output file path> "QUERY"


