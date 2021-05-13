package core.lsh;

import java.util.Random;

public class HashFunction {
    private final Random randomGenerator;
    private final Entry randomVector;
    private final double randomBias;
    private final double w;

    public HashFunction(int dimensions, int w) {
        randomGenerator = new Random();
        this.w = w;
        this.randomBias = getRandomDouble(0, w);

        double[] randomValues = new double[dimensions];
        for(int d = 0; d < dimensions; d ++) {
            randomValues[d] = randomGenerator.nextGaussian();
        }

        randomVector = new Entry(randomValues);
    }

    private double getRandomDouble(int lowerBound, int upperBound) {
        // Note that the upper and lower bounds are inclusive.
        if (lowerBound < 0 || upperBound < 0) {
            throw new IllegalArgumentException("Lower and upper bounds must be equal or greater than 0.");
        } else if (upperBound <= lowerBound) {
            throw new IllegalArgumentException("Upper bound must be greater than lower bound.");
        }

        return randomGenerator.nextDouble() * (upperBound - lowerBound) + lowerBound;
    }

    public int hash(Entry vector){
        double hashValue = (vector.dot(randomVector) + randomBias) / w;
        return (int) Math.floor(hashValue);
    }
}
