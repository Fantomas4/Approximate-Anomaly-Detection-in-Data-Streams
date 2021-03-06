package core;


import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;

import core.ISBIndex.ISBNode;


public class OutlierDetector {
    protected static final Long FIRST_OBJ_ID = 1L;

    // ID indicating the window's starting object
    protected Long windowStart;
    // ID indicating the window's ending object
    protected Long windowEnd;
    private final TreeSet<Outlier> outliersFound;
    protected int windowSize;
    protected int slideSize;
    // list used to find expired nodes
    protected Vector<ISBNode> windowNodes;

    public OutlierDetector(int windowSize, int slideSize) {
        outliersFound = new TreeSet<Outlier>();

        this.windowSize = windowSize;
        // create nodes list of window
        windowNodes = new Vector<ISBNode>();
        this.windowStart = FIRST_OBJ_ID;
        this.windowEnd = (long) windowSize;
        
        this.slideSize = slideSize;

    }

    protected boolean IsNodeIdInWin(long id) {
        Long start = windowStart;
        if ( (start <= id) && (id <= windowEnd) )
            return true;
        else
            return false;
    }

    public void evaluateRemainingNodesInWin() {
        for (ISBNode node : windowNodes) {
            evaluateAsOutlier(node);
        }
    }

    public void evaluateAsOutlier(ISBNode node) {
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
