package algorithms;

import core.lsh.Entry;
import core.mcodbase.ISBIndex;
import core.OutlierDetector;

import java.util.HashMap;
import java.util.TreeSet;

public class LSHOD extends OutlierDetector {

    protected static class EventItem implements Comparable<MCODBase.EventItem> {
        public ISBIndex.ISBNode node;
        public Long timeStamp;

        public EventItem(ISBIndex.ISBNode node, Long timeStamp) {
            this.node = node;
            this.timeStamp = timeStamp;
        }

        @Override
        public int compareTo(MCODBase.EventItem t) {
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
        public TreeSet<MCODBase.EventItem> setEvents;

        public EventQueue() {
            setEvents = new TreeSet<MCODBase.EventItem>();
        }

        public void Insert(ISBIndex.ISBNode node, Long expTime) {
            setEvents.add(new MCODBase.EventItem(node, expTime));
        }

        public MCODBase.EventItem FindMin() {
            if (setEvents.size() > 0) {
                // events are sorted ascenting by expiration time
                return setEvents.first();
            }
            return null;
        }

        public MCODBase.EventItem ExtractMin() {
            MCODBase.EventItem e = FindMin();
            if (e != null) {
                setEvents.remove(e);
                return e;
            }
            return null;
        }
    }

    protected int nRangeQueriesExecuted = 0;

    // object identifier increments with each new data stream object
    protected Long objId;
    protected MCODBase.EventQueue eventQueue;

    protected double m_radius;
    protected int m_k;
    protected double m_theta = 1.0;

    // statistics
    public int m_nBothInlierOutlier;
    public int m_nOnlyInlier;
    public int m_nOnlyOutlier;

    public LSHOD(int windowSize, int slideSize, double radius, int k) {
        super(windowSize, slideSize);

        m_radius = radius;
        m_k = k;

        objId = FIRST_OBJ_ID; // init object identifier

        // create event queue
        eventQueue = new MCODBase.EventQueue();

        // init statistics
        m_nBothInlierOutlier = 0;
        m_nOnlyInlier = 0;
        m_nOnlyOutlier = 0;
    }

    protected void SetNodeType(Entry entry, Entry.EntryType type) {
        entry.entryType = type;
        // update statistics
        if (type == Entry.EntryType.OUTLIER)
            entry.nOutlier++;
        else
            entry.nInlier++;
    }

    protected void AddToEventQueue(Entry x, Entry entryMinExp) {
        if (entryMinExp != null) {
            Long expTime = GetExpirationTime(entryMinExp);
            eventQueue.Insert(x, expTime);
        }
    }

    protected Long GetExpirationTime(Entry entry) {
        return entry.id + windowSize + 1;
    }

    protected int getNodeSlide(Entry entry) {
        // Since node IDs begin from 1, we subtract 1 from the id so that the integer division
        // operation always returns the correct slide the node belongs to.
        long adjustedID = entry.id - 1;

        // The result is incremented by 1 since the slide index starts from 1.
        return (int)(adjustedID / slideSize) + 1;

    }

    protected void doSlide() {
        windowStart += slideSize;
        windowEnd += slideSize;
    }

    protected boolean IsSafeInlier(Entry entry) {
        return entry.count_after >= m_k;
    }

    protected void SaveOutlier(Entry entry) {
        entry.nOutlier++; // update statistics
    }

    protected void RemoveOutlier(Entry entry) {
        entry.nInlier++; // update statistics
    }

    protected void AddNode(Entry entry) {
        windowNodes.add(entry);
    }

    protected void RemoveNode(ISBIndex.ISBNode node) {
        windowNodes.remove(node);
        // update statistics
        UpdateStatistics(node);
        // Check whether the node should be recorded as a pure outlier
        // by the outlier detector
        evaluateAsOutlier(node);
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
        results.put("nRangeQueriesExecuted", nRangeQueriesExecuted);
        return results;
    }

}
