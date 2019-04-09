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
                .setAppName("Word Count");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("ERROR");

        //Reads the collection of documents into an RDD named docs.
        JavaRDD<String> docs = sc.textFile(args[0]).cache();


        /*
        We want to exclude the time to load the text file
        from our measurements. To do, so we need to force the
        loading to happen before the stopwatch is started so that
        our measure will be accurate.
        */
        docs.count();



        //IMPROVED WORDCOUNT 1_________________________________________________________________________________________

        long start1 = System.currentTimeMillis();

        JavaPairRDD<String, Long> wordcount1 = docs

                // Map phase
                .flatMapToPair((document) -> {
                    String[] tokens = document.split(" ");
                    HashMap<String, Long> counts = new HashMap<>();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();

                    for (String token : tokens) {
                        counts.put(token, 1L + counts.getOrDefault(token, 0L)); //need comments here?
                    }
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })

                //Reduce phase

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
        System.out.println("Elapsed time " + (end1 - start1) + " ms" + "\n");
        //_____________________________________________________________________________________________________________
        /* NOTES TO IMPROVED WORDCOUNT 1
        the Elapsed time for this wordcount is 2339 ms and is the fastest method we tested, let Spark manage the load
        is the best choice
         */


        //IMPROVED WORDCOUNT 2.1_______________________________________________________________________________________

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
                    return pairs.iterator(); // the RDD is now composed by the Key-Value pair (w,c(w))
                })
                .groupBy(it -> ThreadLocalRandom.current().nextInt(0, k)) //we use group by for assign random keys from 0 to k
                // The RDD is now composed by (x,(w,c(w)))
                //Reduce_1 in this reduce phase we want to sum the occurencies for every partition x
                .flatMapToPair((pairsByNumKey) -> {
                    HashMap<String, Long> counts = new HashMap();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (Tuple2<String, Long> pair : pairsByNumKey._2){ //with pairsByNumKey._2 we access to the value, which is <w,c(w)>
                        String word = pair._1;
                        Long counter = pair._2;
                        counts.put(word, counter + counts.getOrDefault(word, 0L));
                    }
                    for(Map.Entry<String, Long> e : counts.entrySet()){
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })

                //Map_2: Identity

                //Reduce_2
                .reduceByKey((x,y) -> x+y); // now we obtain the total count



        System.out.println("Improved Word Count 2.1 found: " + wordcount2.count() + " different words");
        long end2 = System.currentTimeMillis();
        System.out.println("Elapsed time " + (end2 - start2) + " ms" + "\n");
        //_____________________________________________________________________________________________________________
        /* NOTES TO IMPROVED WORDCOUNT 2.1
        the Elapsed time for this wordcount is 4968 ms and is the slowest method we tested, seems that assigning manually
        the random keys is not the best computational choice
         */


        //IMPROVED WORDCOUNT 2.2_______________________________________________________________________________________
        long start3 = System.currentTimeMillis();

        JavaPairRDD<String,Long> wordcount3 = docs
                //Map_1
                .flatMapToPair((document) -> {
                    String[] tokens = document.split(" ");
                    HashMap<String, Long> counts = new HashMap();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        counts.put(token, 1L + counts.getOrDefault(token, 0L));
                    }
                    for (Map.Entry<String, Long> e : counts.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    //pairs contains the updated (w,ci(w))
                    return pairs.iterator();
                })

                .repartition(k) //this time we use a repartition method offered by spark

                //Reduce_1
                .mapPartitionsToPair((it) -> {
                    HashMap<String, Long> counts = new HashMap();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    while (it.hasNext()){ //similarly to before w now access directly to the couple (w,c(w)) in every partition for calculate the intermediate sum
                        Tuple2<String, Long> pair = it.next();
                        String word = pair._1;
                        Long counter = pair._2;
                        counts.put(word, counter + counts.getOrDefault(word, 0L));
                    }
                    for(Map.Entry<String, Long> e : counts.entrySet()){
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })

                //Map_2 (Identity)

                //Reduce_2
                .reduceByKey((x,y) -> x+y); //summing all the intermidiate sums



        System.out.println("Improved Word Count 2.2 found: " + wordcount3.count() + " different words");
        long end3 = System.currentTimeMillis();
        System.out.println("Elapsed time " + (end3 - start3) + " ms" + "\n");
        //_____________________________________________________________________________________________________________
        /* NOTES TO IMPROVED WORDCOUNT 2.2
        the Elapsed time for this wordcount is 3871 ms and yelds better perfomance than the wordcount 2.1, still not as
        good as the wordcount 1
         */


        //COMPUTING THE AVERAGE WORD LENGTH____________________________________________________________________________

        long start4 = System.currentTimeMillis();

        JavaPairRDD<String,Long> wordcount4 = docs
                //Map_1
                .flatMapToPair((document) -> {
                    String[] tokens = document.split(" "); //again we are using hash maps for add the words in the RDD with their relative length
                    HashMap<String, Long> wordLength = new HashMap();
                    ArrayList<Tuple2<String, Long>> pairs = new ArrayList<>();
                    for (String token : tokens) {
                        wordLength.put(token, (long) token.length()); //token.length() returns the length of the word
                    }
                    for (Map.Entry<String, Long> e : wordLength.entrySet()) {
                        pairs.add(new Tuple2<>(e.getKey(), e.getValue()));
                    }
                    return pairs.iterator();
                })
                .distinct(); //this method ensures are that no repetition in the whole RDD occurs
        // the RDD is now formed by (word, wordlength)

        //Reduce_1
        Double average = (double) (wordcount4.values().reduce((x,y) -> (x+y))) / wordcount4.count();
        // the method .values() return the values of an RDD then we can sum up all the values with the well known method .reduce
        // then we just divide the sum obtained by the total number of different words

        System.out.println("The average word length is: " + average);
        long end4 = System.currentTimeMillis();
        System.out.println("Elapsed time " + (end4 - start4) + " ms" + "\n");

        //_____________________________________________________________________________________________________________

        System.out.println("Press enter to finish");
        System.in.read();

    }
}
