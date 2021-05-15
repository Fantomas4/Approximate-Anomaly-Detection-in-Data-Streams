package algorithms;

import core.StreamObj;
import core.lsh.Entry;
import core.OutlierDetector;
import core.lsh.LSHIndex;

import java.util.*;


public class LSHOD extends OutlierDetector<Entry> {

    protected static class EventItem implements Comparable<EventItem> {
        public Entry entry;
        public Long timeStamp;

        public EventItem(Entry entry, Long timeStamp) {
            this.entry = entry;
            this.timeStamp = timeStamp;
        }

        @Override
        public int compareTo(EventItem t) {
            if (this.timeStamp > t.timeStamp) {
                return +1;
            } else if (this.timeStamp < t.timeStamp) {
                return -1;
            } else {
                if (this.entry.id > t.entry.id)
                    return +1;
                else if (this.entry.id < t.entry.id)
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

        public void insert(Entry entry, Long expTime) {
            setEvents.add(new EventItem(entry, expTime));
        }

        public EventItem findMin() {
            if (setEvents.size() > 0) {
                // events are sorted ascenting by expiration time
                return setEvents.first();
            }
            return null;
        }

        public EventItem extractMin() {
            EventItem e = findMin();
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
    protected EventQueue eventQueue;
    // LSH index of entries
    protected LSHIndex lshIndex;

    protected double m_radius;
    protected int m_k;
    protected double m_theta = 1.0;

    // statistics
    public int m_nBothInlierOutlier;
    public int m_nOnlyInlier;
    public int m_nOnlyOutlier;

    public LSHOD(int windowSize, int slideSize,  double radius, int k, int dimensions, int numberOfHashes, int numberOfHashTables, int w) {
        super(windowSize, slideSize);

        m_radius = radius;
        m_k = k;

        objId = FIRST_OBJ_ID; // init object identifier

        // create LSH Index
        lshIndex = new LSHIndex(numberOfHashes, numberOfHashTables, w, dimensions);

        // create event queue
        eventQueue = new EventQueue();

        // init statistics
        m_nBothInlierOutlier = 0;
        m_nOnlyInlier = 0;
        m_nOnlyOutlier = 0;
    }

    protected void setNodeType(Entry entry, Entry.EntryType type) {
        entry.entryType = type;
        // update statistics
        if (type == Entry.EntryType.OUTLIER)
            entry.nOutlier++;
        else
            entry.nInlier++;
    }

    protected void addToEventQueue(Entry x, Entry entryMinExp) {
        if (entryMinExp != null) {
            Long expTime = getExpirationTime(entryMinExp);
            eventQueue.insert(x, expTime);
        }
    }

    protected Long getExpirationTime(Entry entry) {
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

    protected boolean isSafeInlier(Entry entry) {
        return entry.count_after >= m_k;
    }

    protected void addNode(Entry entry) {
        windowElements.add(entry);
    }

    protected void removeEntry(Entry entry) {
        windowElements.remove(entry);
        // update statistics
        updateStatistics(entry);
        // Check whether the node should be recorded as a pure outlier
        // by the outlier detector
        evaluateAsOutlier(entry);
    }

    protected void updateStatistics(Entry entry) {
        if ((entry.nInlier > 0) && (entry.nOutlier > 0))
            m_nBothInlierOutlier++;
        else if (entry.nInlier > 0)
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
        for (Entry entry : windowElements) {
            if ((entry.nInlier > 0) && (entry.nOutlier > 0))
                nBothInlierOutlier++;
            else if (entry.nInlier > 0)
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

    void addNeighbor(Entry entry, Entry q, boolean bUpdateState) {
        // check if q still in window
        if (isElemInWindow(q.id) == false) {
            return;
        }

        if (getNodeSlide(q) >= getNodeSlide(entry)) {
            entry.count_after ++;
        } else {
            entry.addPrecNeigh(q);
        }
//        if (q.id < node.id) {
//            node.AddPrecNeigh(q);
//        } else {
//            node.count_after++;
//        }

        if (bUpdateState) {
            // check if entry is an inlier or outlier
            int count = entry.count_after + entry.countPrecNeighs(windowStart);
            if ((entry.entryType == Entry.EntryType.OUTLIER) && (count >= m_k)) {
                // remove entry from outliers
                // mark entry as an inlier
                setNodeType(entry, Entry.EntryType.INLIER);
                // If entry is an unsafe inlier, insert it to the event queue
                if (!isSafeInlier(entry)) {
                    Entry entryMinExp = entry.getMinPrecNeigh(windowStart);
                    addToEventQueue(entry, entryMinExp);
                }
            }
        }
    }

    void processNewEntry(Entry entryNew) {
        // Perform R range query in LSH Index to find the points relatively close to entryNew.
        List<Entry> resultEntries = lshIndex.query(entryNew);
        nRangeQueriesExecuted ++;

        for (Entry resultEntry : resultEntries) {
            // Add the neighbors found by the range query to entryNew
            addNeighbor(entryNew, resultEntry, false);

            // Update new entryNew's neighbors by adding entryNew to them
            addNeighbor(resultEntry, entryNew, true);
        }

        // Add entryNew to the LSH Index
        lshIndex.insert(entryNew);

        // Check if nodeNew is an inlier or outlier
        int count = entryNew.countPrecNeighs(windowStart) + entryNew.count_after;
        if (count >= m_k) {
            // nodeNew is an inlier
            setNodeType(entryNew, Entry.EntryType.INLIER);
            // If nodeNew is an unsafe inlier, insert it to the event queue
            if (!isSafeInlier(entryNew)) {
                Entry entryMinExp = entryNew.getMinPrecNeigh(windowStart);
                addToEventQueue(entryNew, entryMinExp);
            }
        } else {
            // nodeNew is an outlier
            setNodeType(entryNew, Entry.EntryType.OUTLIER);
        }
    }

    void processEventQueue(Entry entryExpired) {
        EventItem e = eventQueue.findMin();
        while ((e != null) && (e.timeStamp <= windowEnd)) {
            e = eventQueue.extractMin();
            Entry x = e.entry;
            // node x must be in window and not in any micro-cluster
            boolean bValid = isElemInWindow(x.id);
            if (bValid) {
                // remove nodeExpired from x.nn_before
                x.removePrecNeigh(entryExpired);
                // get amount of neighbors of x
                int count = x.count_after + x.countPrecNeighs(windowStart);
                if (count < m_k) {
                    // x is an outlier
                    setNodeType(x, Entry.EntryType.OUTLIER);
                } else {
                    // If x is an unsafe inlier, add it to the event queue
                    if (!isSafeInlier(x)) {
                        // get oldest preceding neighbor of x
                        Entry entryMinExp = x.getMinPrecNeigh(windowStart);
                        // add x to event queue
                        addToEventQueue(x, entryMinExp);
                    }
                }
            }
            e = eventQueue.findMin();
        }
    }

    void processExpiredNodes(ArrayList<Entry> expiredEntries) {
        for (Entry expiredEntry : expiredEntries) {
            // Remove expiredEntry from LSH Index
            lshIndex.remove(expiredEntry);

            removeEntry(expiredEntry);
            processEventQueue(expiredEntry);
        }
    }

    public void processNewStreamObjects(ArrayList<StreamObj> streamObjs) {
        if (windowElements.size() >= windowSize) {
            // If the window is full, perform a slide
            doSlide();
            // Process expired nodes
            processExpiredNodes(getExpiredEntries());
        }

        // Process new nodes
        for (StreamObj streamObj : streamObjs) {
            Entry entryNew = new Entry(objId, streamObj.getValues(), streamObj); // create new ISB node
            addNode(entryNew); // add nodeNew to window
            processNewEntry(entryNew);

            objId++; // update object identifier
        }


        // DIAG ONLY -- DELETE
        System.out.println("------------------------ LSHOD ------------------------");
        System.out.println("DIAG - Current stream object: " + (objId - 1));
        System.out.println("DIAG - TEMP OUTLIER SET SIZE: " + getOutliersFound().size());
        System.out.println("DIAG - TEMP Window size is: " + windowElements.size());
        System.out.println("-------------------------------------------------------");
    }

    private ArrayList<Entry> getExpiredEntries() {
        ArrayList<Entry> expiredNodes = new ArrayList<>();
        for (Entry entry : windowElements) {
            // check if node has expired
            if (entry.id < windowStart) {
                expiredNodes.add(entry);
            } else {
                break;
            }
        }
        return expiredNodes;
    }

}
