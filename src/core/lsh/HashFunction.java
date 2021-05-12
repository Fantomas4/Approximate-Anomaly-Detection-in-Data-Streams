package core.lsh;

import java.util.Random;

public class HashFunction {
    private final Entry randomVector;
    private final double randomBias;
    private final double w;

    public HashFunction(int dimensions, double w) {
        Random rand = new Random();
        this.w = w;
        this.randomBias = rand.nextDouble();

        double[] randomValues = new double[dimensions];
        for(int d = 0; d < dimensions; d ++) {
            randomValues[d] = rand.nextGaussian();
        }

        randomVector = new Entry(randomValues);
    }

    public int hash(Entry vector){
        double hashValue = (vector.dot(randomVector) + randomBias) / w;
        return (int) Math.floor(hashValue);
    }
}
