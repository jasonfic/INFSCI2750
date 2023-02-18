To execute the n-gram frequency counter program:

-Run 'hadoop jar MiniProject1-mapreduce-v6.jar group4.MiniProject1.HadoopNgramFrequency
<input file> <output directory> <n> <max byte size>' in the directory that stores the JAR file.
-<input file> should be the name of an example text file, such as helloworld.txt, or the product of a RandomTextWriter operation, such as part-m-00000.
-<output directory> should be the name of an hdfs directory that does not already exist.
-<n> should be an integer representing the size of the chunks of characters within the input text you want to analyze the frequency of. If you leave this value blank, it will default to 1.
-<max byte size> should be an integer representing the maximum amount of bytes that should be read from the input file. This is to prevent performance issues in which the program crashes due to a lack of processing memory. RandomTextWriter writes files that are mutliple GB large by default, so this parameter allows randomly-written files to work with the program. By default, the maximum byte value is 5000.
