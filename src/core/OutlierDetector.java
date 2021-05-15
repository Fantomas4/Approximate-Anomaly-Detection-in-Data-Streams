package core;


import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;


public class OutlierDetector<T extends DataObj<T>> {
    protected static final Long FIRST_OBJ_ID = 1L;

    // ID indicating the window's starting object
    protected Long windowStart;
    // ID indicating the window's ending object
    protected Long windowEnd;
    private final TreeSet<Outlier<T>> outliersFound;
    protected int windowSize;
    protected int slideSize;
    // list used to find expired nodes
    public Vector<T> windowElements;

    public OutlierDetector(int windowSize, int slideSize) {
        outliersFound = new TreeSet<>();

        this.windowSize = windowSize;
        // create nodes list of window
        windowElements = new Vector<>();
        this.windowStart = FIRST_OBJ_ID;
        this.windowEnd = (long) windowSize;
        
        this.slideSize = slideSize;

    }

    protected boolean isElemInWindow(long id) {
        Long start = windowStart;
        if ( (start <= id) && (id <= windowEnd) )
            return true;
        else
            return false;
    }

    public void evaluateRemainingElemsInWin() {
        for (T elem : windowElements) {
            evaluateAsOutlier(elem);
        }
    }

    public void evaluateAsOutlier(T elem) {
        if (elem.nOutlier > 0 && elem.nInlier == 0) {
            // node is a pure outlier, so we record it
            recordOutlier(new Outlier<>(elem));
        }
    }

    private void recordOutlier(Outlier<T> newOutlier) {
        outliersFound.add(newOutlier);
    }

    public Set<Outlier<T>> GetOutliersFound() {
        return outliersFound;
    }



}
