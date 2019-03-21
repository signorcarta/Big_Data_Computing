import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Scanner;

public class G10HM1 {

  // This workaround explicitly define a class implementing both Comparator and Serializable
  public static class Maximum implements Serializable, Comparator<Double> {

    public int compare(Double a, Double b) {
      if (a < b) return -1;
      else if (a > b) return 1;
      return 0;

    }
  }

    // Main
    public static void main(String[] args) throws FileNotFoundException {
      if (args.length == 0) {
        throw new IllegalArgumentException("Expecting the file name on the command line");
      }

      // Read a list of numbers from the program options
      ArrayList<Double> lNumbers = new ArrayList<>();
      Scanner s = new Scanner(new File(args[0]));
      while (s.hasNext()) {
        lNumbers.add(Double.parseDouble(s.next()));
      }
      s.close();

      // Setup Spark
      SparkConf conf = new SparkConf(true)
              .setAppName("Preliminaries");
      JavaSparkContext sc = new JavaSparkContext(conf);

      // Create a parallel collection
      JavaRDD<Double> dNumbers = sc.parallelize(lNumbers);

      //Find the max value using the reduce method of the RDD interface.
      double max1 = dNumbers.reduce((x,y) -> Math.max(x,y));
      System.out.println("The max computed using the reduce method is: " + max1);

      //Find the max value in dNumbers using the max method of the RDD interface.
      double max2 = dNumbers.max(new Maximum());
      System.out.println("The max computed using the max method is: " + max2);

      //Find the min value in dNumbers using the max method of the RDD interface, to be used afterward.
      double min = dNumbers.min(new Maximum());

      /*  Creates a new RDD containing the values of dNumbers normalized in [0,1].
          A simple division of each element by the max, in the RDD still normalizes the values,
          but would actually produce in output a min > 0.
          This way of normalization we choose instead, normalizes the values of our dataset
          giving a min = 0 and a max = 1, with the remaining values in between. Note that this
          choice of normalization works even for datasets containing negative numbers.
       */
      JavaRDD<Double> dNormalized = dNumbers.map((x) ->  ((x - min)/(max2 - min)));

      //Creates and prints a new RDD containing only the that are below the average of the normalized dataset.
      double mean = dNormalized.reduce((x,y) -> (x + y)) / dNormalized.count();
      JavaRDD<Double> oddNumbers = dNormalized.filter((x) -> (x < mean));
      long hmany = oddNumbers.count();
      //Brings all the data contained in this RDD to the driver.
      String nums = "";
      for(Double line:oddNumbers.collect()){
        nums += line.toString() + "  ";
      }

      //Creates and prints a new RDD containing a uniformly distributed sample of the data.
      //Tests the sampling method by a fraction over 0.5.
      JavaRDD<Double> sampled = dNormalized.sample(false, 0.5);
      long hmany2 = sampled.count();
      String nums2= "";
      for(Double line:sampled.collect()){
        nums2 += line.toString() + "  ";
      }

      System.out.println("The " + hmany + " numbers that passed the filter selection are: " + nums);
      System.out.println("The " + hmany2 + " numbers after the sampling selection are: " + nums2);

    }

  }

