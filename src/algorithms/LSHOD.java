package algorithms;

import core.StreamObj;
import core.lsh.Entry;
import core.OutlierDetector;
import core.lsh.LSHIndex;
import core.lsh.families.EuclideanDistance;
import core.lsh.families.EuclidianHashFamily;

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

        public void Insert(Entry entry, Long expTime) {
            setEvents.add(new EventItem(entry, expTime));
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

    // DIAG ONLY -- DELETE
    int diagSafeInliersCount = 0;

    protected int nRangeQueriesExecuted = 0;

    // object identifier increments with each new data stream object
    protected Long objId;
    protected EventQueue eventQueue;
    // LSH index of entries
    protected LSHIndex lshIndex;
    private EuclideanDistance euclideanDistance;

    protected double m_radius;
    protected int m_k;
    protected double m_theta = 1.0;

    // statistics
    public int m_nBothInlierOutlier;
    public int m_nOnlyInlier;
    public int m_nOnlyOutlier;

    public LSHOD(int windowSize, int slideSize,  double radius, int k, int dimensions, int numberOfHashes, int numberOfHashTables) {
        super(windowSize, slideSize);

        m_radius = radius;
        m_k = k;

        objId = FIRST_OBJ_ID; // init object identifier

        // create LSH Index
        lshIndex = new LSHIndex(new EuclidianHashFamily((int)radius, dimensions), numberOfHashes, numberOfHashTables);
        euclideanDistance = new EuclideanDistance();

        // create event queue
        eventQueue = new EventQueue();

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
        windowElements.add(entry);
    }

    protected void RemoveEntry(Entry entry) {
        windowElements.remove(entry);
        // update statistics
        UpdateStatistics(entry);
        // Check whether the node should be recorded as a pure outlier
        // by the outlier detector
        evaluateAsOutlier(entry);
    }

    protected void UpdateStatistics(Entry entry) {
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

    void AddNeighbor(Entry entry, Entry q, boolean bUpdateState) {
        // check if q still in window
        if (IsElemInWindow(q.id) == false) {
            return;
        }

        if (getNodeSlide(q) >= getNodeSlide(entry)) {
            entry.count_after ++;
        } else {
            entry.AddPrecNeigh(q);
        }
//        if (q.id < node.id) {
//            node.AddPrecNeigh(q);
//        } else {
//            node.count_after++;
//        }

        if (bUpdateState) {
            // check if entry is an inlier or outlier
            int count = entry.count_after + entry.CountPrecNeighs(windowStart);
            if ((entry.entryType == Entry.EntryType.OUTLIER) && (count >= m_k)) {
                // remove entry from outliers
                RemoveOutlier(entry);
                // mark entry as an inlier
                SetNodeType(entry, Entry.EntryType.INLIER);
                // If entry is an unsafe inlier, insert it to the event queue
                if (!IsSafeInlier(entry)) {
                    Entry entryMinExp = entry.GetMinPrecNeigh(windowStart);
                    AddToEventQueue(entry, entryMinExp);
                }
            }
        }
    }

    void ProcessNewEntry(Entry entryNew) {
        // Perform R range query in LSH Index to find the points relatively close to entryNew.
        List<Entry> queryResults = lshIndex.rangeQuery(entryNew);
        nRangeQueriesExecuted ++;

        List<Entry> resultEntries = new ArrayList<>();
        for (Entry queryResult : queryResults) {
            // Calculate the exact euclidean distance of the points returned by the LSH query
            // from entryNew to determine its precise neighbors.
            if (euclideanDistance.distance(queryResult, entryNew) <= m_radius) {
                resultEntries.add(queryResult);
            } else {
                // Since the points returned in queryResult are order in an ascending order based on their distance from
                // entryNew, if a point if fount to have a distance greater than R from entryNew, the iteration is terminated.
                break;
            }
        }

        for (Entry resultEntry : resultEntries) {
            // Add the neighbors found by the range query to entryNew
            AddNeighbor(entryNew, resultEntry, false);

            // Update new entryNew's neighbors by adding entryNew to them
            AddNeighbor(resultEntry, entryNew, true);
        }

        // Add entryNew to the LSH Index
        lshIndex.add(entryNew);

        // Check if nodeNew is an inlier or outlier
        int count = entryNew.CountPrecNeighs(windowStart) + entryNew.count_after;
        if (count >= m_k) {
            // nodeNew is an inlier
            SetNodeType(entryNew, Entry.EntryType.INLIER);
            // If nodeNew is an unsafe inlier, insert it to the event queue
            if (!IsSafeInlier(entryNew)) {
                Entry entryMinExp = entryNew.GetMinPrecNeigh(windowStart);
                AddToEventQueue(entryNew, entryMinExp);
            }
        } else {
            // nodeNew is an outlier
            SetNodeType(entryNew, Entry.EntryType.OUTLIER);
            SaveOutlier(entryNew);
        }
    }

    void ProcessEventQueue(Entry entryExpired) {
        // DIAG ONLY -- DELETE
        diagSafeInliersCount = 0;

        EventItem e = eventQueue.FindMin();
        while ((e != null) && (e.timeStamp <= windowEnd)) {
            e = eventQueue.ExtractMin();
            Entry x = e.entry;
            // node x must be in window and not in any micro-cluster
            boolean bValid = IsElemInWindow(x.id);
            if (bValid) {
                // remove nodeExpired from x.nn_before
                x.RemovePrecNeigh(entryExpired);
                // get amount of neighbors of x
                int count = x.count_after + x.CountPrecNeighs(windowStart);
                if (count < m_k) {
                    // x is an outlier
                    SetNodeType(x, Entry.EntryType.OUTLIER);
                    SaveOutlier(x);
                } else {
                    // DIAG ONLY -- DELETE
                    if (x.count_after >= m_k) diagSafeInliersCount++;

                    // If x is an unsafe inlier, add it to the event queue
                    if (!IsSafeInlier(x)) {
                        // get oldest preceding neighbor of x
                        Entry entryMinExp = x.GetMinPrecNeigh(windowStart);
                        // add x to event queue
                        AddToEventQueue(x, entryMinExp);
                    }
                }
            }
            e = eventQueue.FindMin();
        }
    }

    void ProcessExpiredNodes(ArrayList<Entry> expiredEntries) {
        for (Entry expiredEntry : expiredEntries) {
            // Remove expiredEntry from LSH Index
            lshIndex.remove(expiredEntry);

            RemoveEntry(expiredEntry);
            ProcessEventQueue(expiredEntry);
        }
    }

    public void ProcessNewStreamObjects(ArrayList<StreamObj> streamObjs) {
        if (windowElements.size() >= windowSize) {
            // If the window is full, perform a slide
            doSlide();
            // Process expired nodes
            ProcessExpiredNodes(GetExpiredEntries());
        }

        // Process new nodes
        for (StreamObj streamObj : streamObjs) {
            Entry entryNew = new Entry(null, objId, streamObj.getValues(), streamObj); // create new ISB node
            AddNode(entryNew); // add nodeNew to window
            ProcessNewEntry(entryNew);

            objId++; // update object identifier
        }


        // DIAG ONLY -- DELETE
        System.out.println("------------------------ LSHOD ------------------------");
        System.out.println("DIAG - Current stream object: " + (objId - 1));
        System.out.println("DIAG - #Safe inliers detected: " + diagSafeInliersCount);
        System.out.println("DIAG - TEMP OUTLIER SET SIZE: " + GetOutliersFound().size());
        System.out.println("DIAG - TEMP Window size is: " + windowElements.size());
        System.out.println("-------------------------------------------------------");
    }

    private ArrayList<Entry> GetExpiredEntries() {
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
