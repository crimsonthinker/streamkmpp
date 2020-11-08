package moa.clusterers.streamkm;

import java.util.List;
import java.util.ArrayList;
import java.lang.Math;

public class Measure {
    public static double euclideanDistance(List<Double> a, List<Double> b) {
        int dimensionA = a.size();
        int dimensionB = b.size();

        if (dimensionA == dimensionB) {
            double sumab = 0.0;
            int index = 0;
            while (index < dimensionA) {
                sumab = sumab + Math.pow(a.get(index) - b.get(index), 2);
                index = index + 1;
            }
            return Math.sqrt(sumab);
        }
        return -1.0;
    }
}
