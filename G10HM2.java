import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;//add this import for the JavaPair
import org.apache.spark.api.java.JavaSparkContext;
import java.util.HashMap;//add for the HashMap
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Map;// add for Map
import scala.Tuple2;// add for Tuple2

public class G10HM2 {

    public static void main(String[] args) throws FileNotFoundException {
        if (args.length == 0) {
            throw new IllegalArgumentException("Expecting the file name on the command line");
        }

        //Gets the number of partition, which is given in input
        int k = Integer.parseInt(args[0]);

        // Setup Spark
        SparkConf conf = new SparkConf(true)
                .setAppName("Preliminaries");
        JavaSparkContext sc = new JavaSparkContext(conf);

        /*
        Reads the collection of documents into an RDD docs.

        [ Note that if a path to a directory rather than to
        a file is passed to textFile, it will load all files
        found in the directory into the RDD. ]
         */
        JavaRDD<String> docs = sc.textFile("filepath").cache();
        //Subdivides the collection into K partitions;
        docs.repartition(k);
        
        /*
        We want to exclude the time to load the text file
        from our measurements. To do so we need to force the
        loading to happen before the stopwatch is started so that
        our measure will be accurate.
        */
        docs.count();

        

        //IMPROVED WORDCOUNT 1________________________________________________________

        long start1 = System.currentTimeMillis();

        JavaPairRDD<String, Long> wordcountpairs = docs
                // Map phase
                .flatMapToPair((document) -> {
                    String[] tokens = document.split(" ");
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();

                    for (String token : tokens) {
                        counts.put(token, 1L + counts.getOrDefault(token, 0L));
                    }
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })

                // Reduce phase
                .groupByKey()
                .mapValues((it) -> {
                  long sum = 0;
                  for (long c : it) {
                    sum += c;
                  }
                  return sum;
                });

        System.out.println("Improved Word Count 1 found: " + wordcountpairs.count() + " unique words");
        long end1 = System.currentTimeMillis();
        System.out.println("Elapsed time " + (end1 - start1) + " ms");
        //_______________________________________________________________________________





        //The following lines have to be the last part of the main method
        System.out.println("Press enter to finish");
        System.in.read();
    }
}
