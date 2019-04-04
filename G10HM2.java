import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.HashMap;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import scala.Tuple2;

import static org.apache.spark.api.java.JavaRDDLike$class.mapPartitionsToPair;

public class G10HM2 {

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            throw new IllegalArgumentException("Expecting the file name on the command line");
        }

        //Gets the number of partition, which is given in input
        int k = 35; //[args[0]] ?

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

                // Map phase.
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

                /*
                Reduce phase.
                With reduceByKey() method, the word count is computed in 2187ms
                */

                .reduceByKey((x,y)->x+y);

                /*

                [Reduce phase with groupByKey() method, takes 3271ms]

                .groupByKey()
                .mapValues((it) -> { //this method requires also a mapValues
                long sum = 0;
                for (long c : it) {
                sum += c;
                }
                return sum;
                });
                */

        System.out.println("Improved Word Count 1 found: " + wordcountpairs.count() + " unique words");
        long end1 = System.currentTimeMillis();
        System.out.println("Elapsed time " + (end1 - start1) + " ms");
        //_______________________________________________________________________________



        //IMPROVED WORDCOUNT 2.1________________________________________________________

        long start2 = System.currentTimeMillis();

        //Map_1
        JavaPairRDD<String, Long> word_count_pairs = docs

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
                .groupBy(it -> ThreadLocalRandom.current().nextInt(0, k))

        //Reduce_1
                .mapValues(x -> x._2);

                    /*
                    for (String token : tokens) {

                        counts.put([LONG]token, 1L + counts.getOrDefault(token, 0L));
                    }
                    for (Map.Entry<Long, Tuple2<String, Long>> e : counts.entrySet()) {

                        pairs.add((int) ThreadLocalRandom.current().nextLong(sqrtN), new Tuple2<>(e.getKey(), e.getValue()));
                    }

                    return pairs.iterator();
                });
                    */

        //Reduce_1




        System.out.println("Improved Word Count 1 found: " + wordcountpairs.count() + " different words");
        long end2 = System.currentTimeMillis();
        System.out.println("Elapsed time " + (end2 - start2) + " ms");
        //_______________________________________________________________________________



        //The following lines have to be the last part of the main method
        System.out.println("Press enter to finish");
        System.in.read();
    }
}

