import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Random;

import static org.apache.spark.mllib.linalg.Vectors.dense;
import static org.apache.spark.mllib.linalg.Vectors.sqdist;

public class G10HM3 {
    //__________________________________________________________________________________________________________________
    public static Vector strToVector(String str) {
        String[] tokens = str.split(" ");
        double[] data = new double[tokens.length];
        for (int i=0; i<tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        return (Vector) dense(data);
    }
    //__________________________________________________________________________________________________________________



    //__________________________________________________________________________________________________________________
    public static ArrayList<Vector> readVectorsSeq(String filename) throws IOException {
        if (Files.isDirectory(Paths.get(filename))) {
            throw new IllegalArgumentException("readVectorsSeq is meant to read a single file.");
        }
        ArrayList<Vector> result = new ArrayList<>();
        Files.lines(Paths.get(filename))
                .map(str -> strToVector(str))
                .forEach(e -> result.add(e));
        return result;
    }
    //__________________________________________________________________________________________________________________



    //__________________________________________________________________________________________________________________
    public static ArrayList<Vector> kmeansPP( ArrayList<Vector> Pi, ArrayList<Long> WPi, int k, int iter){

        //Initializations
        ArrayList<Vector> P = new ArrayList<Vector>(Pi); //Dataset
        ArrayList<Long> WP = new ArrayList<Long>(WPi); //Array of weights
        ArrayList<Vector> C = new ArrayList<Vector>(k);//Equivalent to S in the slides
        ArrayList<Double> curMinDist = new ArrayList<>();

        double distSum = 0;
        double randNum;
        double[] probDist;

        long objFunction;

        //Generate random number n_____________________________________________________________________
        Random rand = new Random();
        int n = rand.nextInt(P.size());
        //Random point chosen from P
        C.add(P.get(n)); //First center selected
        P.remove(n); //Remove it from the initial set
        WP.remove(n);
        //_____________________________________________________________________________________________

        //Main For cycle_______________________________________________________________________________
        for(int i=1; i<k; k++){

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



        //Refine C with Lloyd's algorithm_______________________________________________________________

        long curDist = 100000000;

        // Contains indexes of cluster belonging, for each point of the dataset
        ArrayList<Integer> partition = new ArrayList<>(P.size());

        //Partition(P, C)__________________________________________________________

        //Cycling over all points considering i-th center
        for(int j=0; j<P.size(); j++){

            // Cyclying over all centers
            for(int i=0; i<C.size(); i++){
                long thisDist = (long) Vectors.sqdist(C.get(i), P.get(j));

                // Compares previous distance (Point[i] - Center) to current
                if( thisDist < curDist){
                    curDist = thisDist;
                    partition.set(j, i); // Currently assigning the j-th point to cluster number i
                }

            }
        }
        //__________________________________________________________________


        //Centroid update___________________________________________________

        for (int j=0; j<P.size(); j++){
            int curClust = partition.get(0);
        }

        //__________________________________________________________________

        //_____________________________________________________________________________________________
        return C; //Returns the initial set of C points [RENAME C WITH C_REFINED] !!!!!!!!!!!!!!!!!!!!!
    }
    //__________________________________________________________________________________________________________________


    /*
    //__________________________________________________________________________________________________________________
    public static double kmeansObj(ArrayList<Vector> P, ArrayList<Vector> C) {
        double sumDist = 0;
        double closest = Double.MAX_VALUE;
        double dist;

        // For every point in P find its nearest center and then sum up into sumDist variable
        for (int i = 0; i < P.size(); i++)
        {
            for (int j = 0; j < C.size(); j++)
            {
                dist = Vectors.sqdist(P.get(i), C.get(j));
                if (dist < closest)
                {
                    closest = dist;
                }
            }
            sumDist += closest;
            closest = Double.MAX_VALUE;
        }
        return sumDist/(P.size());
    }
    //__________________________________________________________________________________________________________________
    */


    //__________________________________________________________________________________________________________________
    public static void main(String[] args){

    }
    //__________________________________________________________________________________________________________________
}