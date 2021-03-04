package algorithms;

import core.ISBIndex;
import core.MTreeMicroClusters;
import core.MicroCluster;
import core.Outlier;
import core.OutlierDetector;
import core.ISBIndex.ISBNode;

import java.util.HashMap;
import java.util.TreeSet;
import java.util.Vector;

public class MCODBase extends OutlierDetector {
    protected static class EventItem implements Comparable<EventItem> {
        public ISBIndex.ISBNode node;
        public Long timeStamp;

        public EventItem(ISBIndex.ISBNode node, Long timeStamp) {
            this.node = node;
            this.timeStamp = timeStamp;
        }

        @Override
        public int compareTo(EventItem t) {
            if (this.timeStamp > t.timeStamp) {
                return +1;
            } else if (this.timeStamp < t.timeStamp) {
                return -1;
            } else {
                if (this.node.id > t.node.id)
                    return +1;
                else if (this.node.id < t.node.id)
                    return -1;
            }
            return 0;
        }
    }

    protected static class EventQueue {
        public TreeSet<EventItem> setEvents;

        public EventQueue() {
            setEvents = new TreeSet<EventItem>();
        }

        public void Insert(ISBIndex.ISBNode node, Long expTime) {
            setEvents.add(new EventItem(node, expTime));
        }

        public EventItem FindMin() {
            if (setEvents.size() > 0) {
                // events are sorted ascenting by expiration time
                return setEvents.first();
            }
            return null;
        }

        public EventItem ExtractMin() {
            EventItem e = FindMin();
            if (e != null) {
                setEvents.remove(e);
                return e;
            }
            return null;
        }
    }

    protected class SearchResultMC {
        public MicroCluster mc;
        public double distance;

        public SearchResultMC(MicroCluster mc, double distance) {
            this.mc = mc;
            this.distance = distance;
        }
    }

    protected int nRangeQueriesExecuted = 0;

    protected static final Long FIRST_OBJ_ID = 1L;

    // object identifier increments with each new data stream object
    protected Long objId;
    // list used to find expired nodes
    protected Vector<ISBNode> windowNodes;
    protected EventQueue eventQueue;
    // MTree index of micro-clusters
    protected MTreeMicroClusters mtreeMC;
    // set of micro-clusters (for trace)
    protected TreeSet<MicroCluster> setMC;
    // nodes treated as new nodes when a mc removed
    protected TreeSet<ISBIndex.ISBNode> nodesReinsert;
    // index of objects not in any micro-cluster
    protected ISBIndex ISB_PD;
    // ID indicating the window's starting object
    protected Long windowStart = FIRST_OBJ_ID;
    // ID indicating the window's ending object
    protected Long windowEnd = (long) windowSize;
    protected double m_radius;
    protected int m_k;
    protected double m_theta = 1.0;

    // statistics
    public int m_nBothInlierOutlier;
    public int m_nOnlyInlier;
    public int m_nOnlyOutlier;

    public MCODBase(int windowSize, int slideSize, double radius, int k) {
        super(windowSize, slideSize);

        m_radius = radius;
        m_k = k;

        System.out.println("Init MCOD:");
        System.out.println("   window_size: " + windowSize);
        System.out.println("   slide_size: " + slideSize);
        System.out.println("   radius: " + m_radius);
        System.out.println("   k: " + m_k);

        objId = FIRST_OBJ_ID; // init object identifier
        // create nodes list of window
        windowNodes = new Vector<ISBNode>();
        // create ISB
        ISB_PD = new ISBIndex(m_radius, m_k);
        // create helper sets for micro-cluster management
        setMC = new TreeSet<MicroCluster>();
        // micro-cluster index
        mtreeMC = new MTreeMicroClusters();
        // create event queue
        eventQueue = new EventQueue();

        // init statistics
        m_nBothInlierOutlier = 0;
        m_nOnlyInlier = 0;
        m_nOnlyOutlier = 0;
    }

    protected void SetNodeType(ISBIndex.ISBNode node, ISBIndex.ISBNode.NodeType type) {
        node.nodeType = type;
        // update statistics
        if (type == ISBIndex.ISBNode.NodeType.OUTLIER)
            node.nOutlier++;
        else
            node.nInlier++;
    }

    protected void AddToEventQueue(ISBIndex.ISBNode x, ISBIndex.ISBNode nodeMinExp) {
        if (nodeMinExp != null) {
            Long expTime = GetExpirationTime(nodeMinExp);
            eventQueue.Insert(x, expTime);
        }
    }

    protected Long GetExpirationTime(ISBNode node) {
        return node.id + windowSize;
    }

    protected void doSlide() {
        if (windowNodes.size() > windowSize) {
            windowStart += slideSize;
            windowEnd += slideSize;
        }
    }

    protected void SaveOutlier(ISBIndex.ISBNode node) {
        AddOutlier(new Outlier(node));
        node.nOutlier++; // update statistics
    }

    protected void RemoveOutlier(ISBIndex.ISBNode node) {
        RemoveOutlier(new Outlier(node));
        node.nInlier++; // update statistics
    }

    protected boolean IsNodeIdInWin(long id) {
        Long start = windowStart;
        if ( (start <= id) && (id <= windowEnd) )
            return true;
        else
            return false;
    }

    protected void AddNode(ISBIndex.ISBNode node) {
        windowNodes.add(node);
    }

    protected void RemoveNode(ISBIndex.ISBNode node) {
        windowNodes.remove(node);
        // update statistics
        UpdateStatistics(node);
    }

    protected void UpdateStatistics(ISBIndex.ISBNode node) {
        if ((node.nInlier > 0) && (node.nOutlier > 0))
            m_nBothInlierOutlier++;
        else if (node.nInlier > 0)
            m_nOnlyInlier++;
        else
            m_nOnlyOutlier++;
    }

    public HashMap<String, Integer> getResults() {
        // get counters of expired nodes
        int nBothInlierOutlier = m_nBothInlierOutlier;
        int nOnlyInlier = m_nOnlyInlier;
        int nOnlyOutlier = m_nOnlyOutlier;

        // add counters of non expired nodes still in window
        for (ISBIndex.ISBNode node : windowNodes) {
            if ((node.nInlier > 0) && (node.nOutlier > 0))
                nBothInlierOutlier++;
            else if (node.nInlier > 0)
                nOnlyInlier++;
            else
                nOnlyOutlier++;
        }

        HashMap<String, Integer> results = new HashMap<>();
        results.put("nBothInlierOutlier", nBothInlierOutlier);
        results.put("nOnlyInlier", nOnlyInlier);
        results.put("nOnlyOutlier", nOnlyOutlier);
        return results;
    }

    public int getnRangeQueriesExecuted() {
        return nRangeQueriesExecuted;
    }

    protected double GetEuclideanDist(ISBIndex.ISBNode n1, ISBIndex.ISBNode n2)
    {
        double diff;
        double sum = 0;
        int d = n1.obj.dimensions();
        for (int i = 0; i < d; i++) {
            diff = n1.obj.get(i) - n2.obj.get(i);
            sum += Math.pow(diff, 2);
        }
        return Math.sqrt(sum);
    }

    protected Vector<SearchResultMC> RangeSearchMC(ISBIndex.ISBNode nodeNew, double radius) {
        Vector<SearchResultMC> results = new Vector<SearchResultMC>();
        // create a dummy mc in order to search w.r.t. nodeNew
        MicroCluster dummy = new MicroCluster(nodeNew);
        // query results are returned ascending by distance
        MTreeMicroClusters.Query query = mtreeMC.getNearestByRange(dummy, radius);
        for (MTreeMicroClusters.ResultItem q : query) {
            results.add(new SearchResultMC(q.data, q.distance));
        }
        return results;
    }
}
