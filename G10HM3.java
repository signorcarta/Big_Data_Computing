import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Random;

public class G10HM3 {
    //__________________________________________________________________________________________________________________
    public static Vector strToVector(String str) {
        String[] tokens = str.split(" ");
        double[] data = new double[tokens.length];
        for (int i=0; i<tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        return (Vector) Vectors.dense(data);
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
        ArrayList<Vector> P = new ArrayList<Vector>(Pi);
        ArrayList<Long> WP = new ArrayList<Long>(WPi);
        ArrayList<Vector> C = new ArrayList<>(k);//Equivalent to S in the slides
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
        P.remove(n); //remove it from the initial set
        WP.remove(n);
        //_____________________________________________________________________________________________

        //Main For cycle_______________________________________________________________________________
        for(int i=1; i<k; k++){

            distSum = 0;

            // Compute the distances of each point from the last center
            for (int j = 0; j < P.size(); j++) {
                double curDist = Math.sqrt(Vectors.sqdist(P.get(j), C.get(i-1)));

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
        objFunction = (long) distSum;
        for(int i=0; i<iter; i++){


        }


        //_____________________________________________________________________________________________
        return C; //Returns the initial set of C points [RENAME C WITH C_REFINED] !!!!!!!!!!!!!!!!!!!!!
    }
    //__________________________________________________________________________________________________________________

    public static void main(String[] args){

    }
}
