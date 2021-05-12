package core.lsh;

import core.DataObj;
import core.StreamObj;

import java.util.*;


public class Entry extends DataObj<Entry> implements Comparable<Entry> {
    public static enum EntryType { OUTLIER, INLIER }
    public EntryType entryType;

    public Entry(double[] values) {
        super(null, values, null);
    }

    public Entry(Long id, double[] values, StreamObj obj){
        super(id, values, obj);

        // init other fields
        InitNode();
    }

    public void InitNode() {
        this.count_after = 0;
        this.entryType = EntryType.OUTLIER;
        this.nn_before = new ArrayList<>();
    }

    @Override
    public int compareTo(Entry t) {
        if (this.id > t.id)
            return +1;
        else if (this.id < t.id)
            return -1;
        return 0;
    }

    public void AddPrecNeigh(Entry entry) {
        int pos = Collections.binarySearch(nn_before, entry);
        if (pos < 0) {
            // item does not exist, so add it to the right position
            nn_before.add(-(pos + 1), entry);
        }
    }

    public void RemovePrecNeigh(Entry entry) {
        int pos = Collections.binarySearch(nn_before, entry);
        if (pos >= 0) {
            // item exists
            nn_before.remove(pos);
        }
    }

    public Entry GetMinPrecNeigh(Long sinceId) {
        if (nn_before.size() > 0) {
            int startPos;
            Entry dummy = new Entry(sinceId, null, null);

            int pos = Collections.binarySearch(nn_before, dummy);
            if (pos < 0) {
                // item does not exist, should insert at position startPos
                startPos = -(pos + 1);
            } else {
                // item exists at startPos
                startPos = pos;
            }

            if (startPos < nn_before.size()) {
                return nn_before.get(startPos);
            }
        }
        return null;
    }

    public int CountPrecNeighs(Long sinceId) {
        if (nn_before.size() > 0) {
            // get number of neighs with id >= sinceId
            int startPos;
            Entry dummy = new Entry(sinceId, null, null);
            int pos = Collections.binarySearch(nn_before, dummy);
            if (pos < 0) {
                // item does not exist, should insert at position startPos
                startPos = -(pos + 1);
            } else {
                // item exists at startPos
                startPos = pos;
            }

            if (startPos < nn_before.size()) {
                return nn_before.size() - startPos;
            }
        }
        return 0;
    }

    public List<Entry> Get_nn_before() {
        return nn_before;
    }

    public double dot (Entry other) {
        double sum = 0;

        for (int d = 0; d < values.length; d++) {
            sum += this.values[d] * other.values[d];
        }

        return sum;
    }





}