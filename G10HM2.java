import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import scala.Tuple2;

public class G10HM2 {

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            throw new IllegalArgumentException("Expecting the file name on the command line");
        }

        //Gets the number of partition, which is given in input
        int k = Integer.parseInt(args[1]);

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
        JavaRDD<String> docs = sc.textFile(args[0]).cache();
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

        JavaPairRDD<String, Long> wordcount1 = docs

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

        System.out.println("Improved Word Count 1 found: " + wordcount1.count() + " unique words");
        long end1 = System.currentTimeMillis();
        System.out.println("Elapsed time " + (end1 - start1) + " ms");
        //_______________________________________________________________________________



        //IMPROVED WORDCOUNT 2.1________________________________________________________

        long start2 = System.currentTimeMillis();

        JavaPairRDD<String, Long> wordcount2 = docs
                //Map_1
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
                .groupBy(it -> ThreadLocalRandom.current().nextInt(0, k)) //it contains (x,(w,c(w)))

                //Reduce_1
                .flatMapToPair((pairsByNumKey) -> {
                    HashMap<String, Long> counts = new HashMap();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (Tuple2<String, Long> pair : pairsByNumKey._2){
                        String word = pair._1;
                        Long counter = pair._2;
                        counts.put(word.toLowerCase(), counter + counts.getOrDefault(word.toLowerCase(), 0L));
                    }
                    for(Map.Entry<String, Long> e : counts.entrySet()){
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })

                //Map_2 (Identity)

                //Reduce_2
                .reduceByKey((x,y) -> x+y);

        System.out.println("Improved Word Count 2.1 found: " + wordcount2.count() + " different words");
        long end2 = System.currentTimeMillis();
        System.out.println("Elapsed time " + (end2 - start2) + " ms");
        //_______________________________________________________________________________



        //IMPROVED WORDCOUNT 2.2________________________________________________________
        long start3 = System.currentTimeMillis();

        JavaPairRDD<String,Long> wordcount3 = docs
                //Map_1
                .flatMapToPair((document) -> {
                    String[] tokens = document.split(" ");
                    HashMap<String, Long> counts = new HashMap();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    //we have added toLowerCase to be not case sensitive
                    for (String token : tokens) {
                        counts.put(token.toLowerCase(), 1L + counts.getOrDefault(token.toLowerCase(), 0L));
                    }
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    //pairs contains the updated (w,ci(w))
                    return pairs.iterator();
                })

                .repartition(k)

                //Reduce_1
                .mapPartitionsToPair((it) -> {
                    HashMap<String, Long> counts = new HashMap();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    while (it.hasNext()){
                        Tuple2<String, Long> pair = it.next();
                        String word = pair._1;
                        Long counter = pair._2;
                        counts.put(word.toLowerCase(), counter + counts.getOrDefault(word.toLowerCase(), 0L));
                    }
                    for(Map.Entry<String, Long> e : counts.entrySet()){
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })

                //Map_2 (Identity)

                //Reduce_2
                .reduceByKey((x,y) -> x+y);

        System.out.println("Improved Word Count 2.2 found: " + wordcount3.count() + " different words");
        long end3 = System.currentTimeMillis();
        System.out.println("Elapsed time " + (end3 - start3) + " ms");
        //_______________________________________________________________________________



        /*COMPUTING THE AVERAGE WORD OCCURRENCE__________________________________________

        JavaPairRDD<String,Long> wordcount4 = docs
                //Map_1
                .flatMapToPair((document) -> {
                            String[] tokens = document.split(" ");
                            HashMap<String, Long> wordLength = new HashMap();
                            ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();

                            for (String token : tokens) {
                                wordLength.put(token, (long) token.length());
                            }

                            for (Map.Entry<String, Long> e : wordLength.entrySet()) {
                                pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                            }

                            return pairs.iterator();
                });

        for (Map.Entry<String, Long> e : wordcount4.entrySet()){

        }






        long average = wordcount4.reduce((x,y)->(x._2 + y._2)) / wordcount3.count();

        //_____________________________________________________________________________*/

        //The following lines have to be the last part of the main method
        System.out.println("Press enter to finish");
        System.in.read();

    }
}

