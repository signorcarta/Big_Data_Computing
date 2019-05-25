import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.BLAS;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;

import static org.apache.spark.mllib.linalg.Vectors.sqdist;
import static org.apache.spark.mllib.linalg.Vectors.zeros;

public class G10HM4
{
    //_________________________________________________________________________________________________________________
    public static void main(String[] args) throws Exception
    {

        //------- PARSING CMD LINE ------------
        // Parameters are:
        // <path to file>, k, L and iter

        if (args.length != 4) {
            System.err.println("USAGE: <filepath> k L iter");
            System.exit(1);
        }
        String inputPath = args[0];
        int k=0, L=0, iter=0;
        try
        {
            k = Integer.parseInt(args[1]);
            L = Integer.parseInt(args[2]);
            iter = Integer.parseInt(args[3]);
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
        if(k<=2 && L<=1 && iter <= 0)
        {
            System.err.println("Something wrong here...!");
            System.exit(1);
        }
        //------------------------------------
        final int k_fin = k;

        //------- DISABLE LOG MESSAGES
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        //------- SETTING THE SPARK CONTEXT      
        SparkConf conf = new SparkConf(true).setAppName("kmedian new approach");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //------- PARSING INPUT FILE ------------
        JavaRDD<Vector> pointset = sc.textFile(args[0], L)
                .map(x-> strToVector(x))
                .repartition(L)
                .cache();
        long N = pointset.count();
        System.out.println("Number of points is : " + N);
        System.out.println("Number of clusters is : " + k);
        System.out.println("Number of parts is : " + L);
        System.out.println("Number of iterations is : " + iter);

        //------- SOLVING THE PROBLEM ------------
        double obj = MR_kmedian(pointset, k, L, iter);
        System.out.println("Objective function is : ---> " + obj + " <---");
    }
    //_________________________________________________________________________________________________________________



    //_________________________________________________________________________________________________________________
    public static Double MR_kmedian(JavaRDD<Vector> pointset, int k, int L, int iter)
    {
        //
        // --- ADD INSTRUCTIONS TO TAKE AND PRINT TIMES OF ROUNDS 1, 2 and 3 
        //

        //------------------------------- ROUND 1 -------------------------------------------------

        JavaRDD<Tuple2<Vector,Long>> coreset = pointset.mapPartitions(x ->
        {
            ArrayList<Vector> points = new ArrayList<>();
            ArrayList<Long> weights = new ArrayList<>();
            while (x.hasNext())
            {
                points.add(x.next());
                weights.add(1L);
            }
            ArrayList<Vector> centers = kmeansPP(points, weights, k, iter);
            ArrayList<Long> weight_centers = compute_weights(points, centers);
            ArrayList<Tuple2<Vector,Long>> c_w = new ArrayList<>();
            for(int i =0; i < centers.size(); ++i)
            {
                Tuple2<Vector, Long> entry = new Tuple2<>(centers.get(i), weight_centers.get(i));
                c_w.add(i,entry);
            }
            return c_w.iterator();
        });

        //-----------------------------------------------------------------------------------------


        //------------------------------- ROUND 2 -------------------------------------------------

        ArrayList<Tuple2<Vector, Long>> elems = new ArrayList<>(k*L);
        elems.addAll(coreset.collect());
        ArrayList<Vector> coresetPoints = new ArrayList<>();
        ArrayList<Long> weights = new ArrayList<>();
        for(int i =0; i< elems.size(); ++i)
        {
            coresetPoints.add(i, elems.get(i)._1);
            weights.add(i, elems.get(i)._2);
        }

        ArrayList<Vector> centers = kmeansPP(points, weights, k, iter); /////////VERIFY THIS LINE/////////

        //-----------------------------------------------------------------------------------------



        //----------------------- ROUND 3: COMPUTE OBJ FUNCTION -----------------------------------

        return kmeansObj(coresetPoints, centers);

        //-----------------------------------------------------------------------------------------
    }
    //_________________________________________________________________________________________________________________



    //_________________________________________________________________________________________________________________
    public static ArrayList<Long> compute_weights(ArrayList<Vector> points, ArrayList<Vector> centers)
    {
        Long weights[] = new Long[centers.size()];
        Arrays.fill(weights, 0L);
        for(int i =0; i < points.size(); ++i)
        {
            double tmp = euclidean(points.get(i), centers.get(0));
            int mycenter = 0;
            for(int j = 1; j < centers.size(); ++j)
            {
                if(euclidean(points.get(i),centers.get(j)) < tmp)
                {
                    mycenter = j;
                    tmp = euclidean(points.get(i), centers.get(j));
                }
            }
            weights[mycenter] += 1L;
        }
        ArrayList<Long> fin_weights = new ArrayList<>(Arrays.asList(weights));
        return fin_weights;
    }
    //_________________________________________________________________________________________________________________



    //_________________________________________________________________________________________________________________
    public static Vector strToVector(String str) {
        String[] tokens = str.split(" ");
        double[] data = new double[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        return Vectors.dense(data);
    }
    //_________________________________________________________________________________________________________________



    //_________________________________________________________________________________________________________________
    // Euclidean distance //
    public static double euclidean(Vector a, Vector b) {
        return Math.sqrt(Vectors.sqdist(a, b));
    }
    //_________________________________________________________________________________________________________________



    //_________________________________________________________________________________________________________________
    public static ArrayList<Vector> kmeansPP( ArrayList<Vector> Pi, ArrayList<Long> WPi, int k, int iter){

        //Initializations
        ArrayList<Vector> P = new ArrayList<Vector>(Pi); //Dataset
        ArrayList<Long> WP = new ArrayList<Long>(WPi); //Array of weights
        ArrayList<Vector> C = new ArrayList<Vector>(k);//Equivalent to S in the slides
        ArrayList<Double> curMinDist = new ArrayList<>();

        double distSum = 0;
        double randNum;
        double[] probDist;

        //Generate random number n_____________________________________________________________________
        Random rand = new Random();
        int n = rand.nextInt(P.size()); //Random point chosen from P
        C.add(P.get(n)); //First center selected
        P.remove(n); //Remove it from the initial point set
        WP.remove(n); //Remove it from the initial weights set
        //_____________________________________________________________________________________________

        //Main For cycle_______________________________________________________________________________
        for(int i=1; i<k; i++){

            distSum = 0;

            // Compute the distances of each point from the last center
            for (int j = 0; j < P.size(); j++) {
                double curDist = Math.sqrt(sqdist(P.get(j), C.get(i-1)));

                // In the first iteration add the entries, otherwise just modify them
                if (C.size() == 1) {
                    curMinDist.add(curDist);
                } else if (curDist < curMinDist.get(j)) {
                    // If a smaller distance is found update curMinDist
                    curMinDist.set(j, curDist);
                }

                // Compute the weighted sum of squared distances
                distSum += curMinDist.get(j) * WP.get(j);
            }

            // Initialize the probability distribution
            probDist = new double[P.size()];
            for (int j = 0; j < P.size(); j++) {
                probDist[j] = WP.get(j) * curMinDist.get(j) / distSum;
            }

            // Draw a point according to the new-found probability distribution
            randNum = rand.nextDouble();
            double leftSum = 0;
            double rightSum = probDist[0];
            for (int j = 1; j <= P.size(); j++) {

                if ((leftSum <= randNum && randNum <= rightSum) || (j == P.size()))
                {
                    // A new point is found. Add it to C while removing it from the other arraylists
                    C.add(P.remove(j-1));
                    curMinDist.remove(j-1);
                    WP.remove(j-1);
                    break;
                }

                // Update the values of the sums
                leftSum = rightSum;
                rightSum += probDist[j];
            }

        }
        //_____________________________________________________________________________________________



        //Refine C with Lloyd's algorithm______________________________________________________________

        ArrayList<Integer> partition = new ArrayList<>(P.size()); // Contains the indexes of the belonging cluster
        //Initialization of the vector Partition
        for (int i = 0; i < P.size(); i++) {
            partition.add(0);
        }

        //Refining centers using Lloyd for "iter" iterations__________________________________
        for(int s=0; s<iter; s++) {

            //Partition(P, C)___________________________________________________

            //Cycling over all points considering i-th center
            for (int j = 0; j < P.size(); j++) {

                long curDist = Long.MAX_VALUE;

                // Cyclying over all centers
                for (int i = 0; i < C.size(); i++) {
                    long thisDist = (long) Vectors.sqdist(C.get(i), P.get(j));

                    // Compares previous distance (Point[i] - Center) to current
                    if (thisDist < curDist) {
                        curDist = thisDist;
                        partition.set(j, i); // Currently assigning the j-th point to cluster with index i
                    }

                }
            }
            //__________________________________________________________________


            //Centroid update___________________________________________________

            for (int i = 0; i < C.size(); i++) {

                long hmany = 0;
                Vector sum = zeros(P.get(144).size());

                for (int j = 0; j < P.size(); j++) {

                    //access the cluster index of the j-th point of the dataset
                    if (i == partition.get(j)) {
                        BLAS.axpy(WP.get(j), P.get(j), sum); //updates sum
                        hmany = hmany + WP.get(j); //updates elements count
                    }
                }

                //Computing new centroid
                BLAS.axpy(1, sum, C.get(i));
                BLAS.scal(1d/hmany,C.get(i));

            }
        }
        //______________________________________________________________________

        //____________________________________________________________________________________

        //_____________________________________________________________________________________________
        return C;
    }
    //_________________________________________________________________________________________________________________



    //_________________________________________________________________________________________________________________
    public static double kmeansObj(ArrayList<Vector> P, ArrayList<Vector> C) {
        double sumDist = 0;
        double closest = Double.MAX_VALUE;
        double dist;

        // For every point in P, find its nearest center and then sum up into sumDist variable
        for (int i = 0; i < P.size(); i++)
        {
            // Scanning all centroids
            for (int j = 0; j < C.size(); j++)
            {
                dist =  Math.sqrt(Vectors.sqdist(P.get(i), C.get(j)));
                if (dist < closest)
                {
                    closest = dist;
                }
            }
            sumDist += closest; //Summing the distance of the closest center
            closest = Double.MAX_VALUE; //Reset closest for next iteration
        }
        return sumDist/(P.size());
    }
    //_________________________________________________________________________________________________________________
}
