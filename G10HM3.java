import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.Vector;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Random;

public class G10HM3 {

    public static Vector strToVector(String str) {
        String[] tokens = str.split(" ");
        double[] data = new double[tokens.length];
        for (int i=0; i<tokens.length; i++) {
            data[i] = Double.parseDouble(tokens[i]);
        }
        return (Vector) Vectors.dense(data);
    }

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

    public static ArrayList<Vector> kmeansPP( ArrayList<Vector> Pi, ArrayList<Long> WPi, int k, int iter){

        //Initializations
        ArrayList<Vector> P = new ArrayList<Vector>(Pi);
        ArrayList<Long> WP = new ArrayList<Long>(WPi);
        ArrayList<Vector> C = new ArrayList<>(k);//Equivalent to S in the slides
        ArrayList<Double> curMinDist = new ArrayList<>();

        double distSum = 0;

        //Generate random number n
        Random rand = new Random();
        int n = rand.nextInt(P.size());

        //Random point chosen from P
        C.add(P.get(n));
        P.remove(n); //remove it from the initial set
        WP.remove(n);

        //Main For cycle________________________________________________________________________________________________
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
                distSum += Math.pow(curMinDist.get(j), 2) * WP.get(j);
            }


        }
        //______________________________________________________________________________________________________________
        return C;
    }

    public static void main(String[] args){

    }
}
