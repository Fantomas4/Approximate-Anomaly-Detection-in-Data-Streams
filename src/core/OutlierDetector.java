package core;


import java.util.Set;
import java.util.TreeSet;
import core.ISBIndex.ISBNode;


public class OutlierDetector {
    private final TreeSet<Outlier> outliersFound;
    protected int windowSize;
    protected int slideSize;
    protected int accessCount = 0; // DIAG ONLY -- DELETE!


    public OutlierDetector(int windowSize, int slideSize) {
        outliersFound = new TreeSet<Outlier>();

        this.windowSize = windowSize;
        this.slideSize = slideSize;
    }

    public void evaluateAsOutlier(ISBNode node) {
        accessCount++;
        if (node.nOutlier > 0 && node.nInlier == 0) {
            // node is a pure outlier, so we record it
            recordOutlier(new Outlier(node));
        }
    }

    private void recordOutlier(Outlier newOutlier) {
        outliersFound.add(newOutlier);
    }

    public Set<Outlier> GetOutliersFound() {
        return outliersFound;
    }



}
