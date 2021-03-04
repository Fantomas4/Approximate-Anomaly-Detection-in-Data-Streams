package core;


import java.util.Set;
import java.util.TreeSet;


public class OutlierDetector {
    private TreeSet<Outlier> outliersFound;


    protected int windowSize;
    protected int slideSize;


    public OutlierDetector(int windowSize, int slideSize) {
        outliersFound = new TreeSet<Outlier>();

        this.windowSize = windowSize;
        this.slideSize = slideSize;
    }

    public Set<Outlier> GetOutliersFound() {
        return outliersFound;
    }

    public void AddOutlier(Outlier newOutlier) {
        outliersFound.add(newOutlier);
    }

    public void RemoveOutlier(Outlier outlier) {
        outliersFound.remove(outlier);
    }
}
