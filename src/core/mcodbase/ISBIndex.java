/*
 *    ISBIndex.java
 *    Copyright (C) 2013 Aristotle University of Thessaloniki, Greece
 *    @author D. Georgiadis, A. Gounaris, A. Papadopoulos, K. Tsichlas, Y. Manolopoulos
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *    
 *    
 */

package core.mcodbase;


import core.DataObj;
import core.StreamObj;

import java.util.*;


public class ISBIndex {
    public static class ISBEntry extends DataObj<ISBEntry> implements Comparable<ISBEntry> {
        public enum EntryType { OUTLIER, INLIER_MC, INLIER_PD }

        public MicroCluster mc;
        public Set<MicroCluster> Rmc;
        public EntryType entryType;


        public ISBEntry(StreamObj obj, Long id) {
            super(id, obj);

            // init other fields
            initEntry();
        }

        public void initEntry() {
            this.mc          = null;
            this.Rmc         = new TreeSet<>();
            this.count_after = 0;
            this.entryType = EntryType.INLIER_PD;
            this.nn_before   = new ArrayList<>();
        }

        @Override
        public int compareTo(ISBEntry t) {
            if (this.id > t.id)
                return +1;
            else if (this.id < t.id)
                return -1;
            return 0;
        }

        public void addPrecNeigh(ISBEntry entry) {
            int pos = Collections.binarySearch(nn_before, entry);
            if (pos < 0) {
                // item does not exist, so add it to the right position
                nn_before.add(-(pos + 1), entry);
            }
        }

        public void removePrecNeigh(ISBEntry entry) {
            int pos = Collections.binarySearch(nn_before, entry);
            if (pos >= 0) {
                // item exists
                nn_before.remove(pos);
            }
        }

        public ISBEntry getMinPrecNeigh(Long sinceId) {
            if (nn_before.size() > 0) {
                int startPos;
                ISBEntry dummy = new ISBEntry(null, sinceId);

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

        public int countPrecNeighs(Long sinceId) {
            if (nn_before.size() > 0) {
                // get number of neighs with id >= sinceId
                int startPos;
                ISBEntry dummy = new ISBEntry(null, sinceId);
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

        public List<ISBEntry> Get_nn_before() {
            return nn_before;
        }
    }

    MTreeStreamObjects mtree;
    Map<Integer, Set<ISBEntry>> mapEntries;
    double m_radius;
    int m_k; // k nearest neighbors

    public ISBIndex(double radius, int k) {
        mtree = new MTreeStreamObjects();
        mapEntries = new HashMap<Integer, Set<ISBEntry>>();
        m_radius = radius;
        m_k = k;
    }

    public int getSize() {
        int nObjects = 0;
        for (Set<ISBEntry> setEntries : mapEntries.values()) {
            nObjects += setEntries.size();
        }

        return nObjects;
    }

    public Vector<ISBEntry> getAllEntries() {
        Vector<ISBEntry> v = new Vector<>();
        Iterator it = mapEntries.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pairs = (Map.Entry) it.next();
            Set<ISBEntry> setEntries = (Set<ISBEntry>) pairs.getValue();
            for (ISBEntry n : setEntries) {
                v.add(n);
            }
        }  
        return v;
    }
    
    public static class ISBSearchResult {
        public ISBEntry entry;
        public double distance;
        
        public ISBSearchResult(ISBEntry n, double distance) {
            this.entry = n;
            this.distance = distance;
        }
    }
    
    public Vector<ISBSearchResult> rangeSearch(ISBEntry entry, double radius) {
        Vector<ISBSearchResult> results = new Vector<>();
        StreamObj obj;
        double d;
        MTreeStreamObjects.Query query = mtree.getNearestByRange(entry.obj, radius);
        for (MTreeStreamObjects.ResultItem q : query) {
            // get next obj found within range
            obj = q.data;
            // get distance of obj from query
            d = q.distance;
            // get all entries referencing obj
            Vector<ISBEntry> entries = mapGetEntries(obj);
            for (int i = 0; i < entries.size(); i++)
                results.add(new ISBSearchResult(entries.get(i), d));
        }        
        return results;
    }
    
    public void insert(ISBEntry entry) {
        // insert object of entry at mtree
        mtree.add(entry.obj);
        // insert entry at map
        mapInsert(entry);
    }
    
    public void remove(ISBEntry entry) {
        // remove from map
        mapDelete(entry);
        // check if stream object at mtree is still being referenced
        if (mapCountObjRefs(entry.obj) <= 0) {
            // delete stream object from mtree
            mtree.remove(entry.obj);
        }
    }
    
    Vector<ISBEntry> mapGetEntries(StreamObj obj) {
        int h = obj.hashCode();
        Vector<ISBEntry> v = new Vector<ISBEntry>();
        if (mapEntries.containsKey(h)) {
            Set<ISBEntry> s = mapEntries.get(h);
            ISBEntry entry;
            Iterator<ISBEntry> i = s.iterator();
            while (i.hasNext()) {
                entry = i.next();
                if (entry.obj.equals(obj))
                    v.add(entry);
            }
        }
        return v;
    }
    
    int mapCountObjRefs(StreamObj obj) {
        int h = obj.hashCode();
        int iCount = 0;
        if (mapEntries.containsKey(h)) {
            Set<ISBEntry> s = mapEntries.get(h);
            ISBEntry n;
            Iterator<ISBEntry> i = s.iterator();
            while (i.hasNext()) {
                n = i.next();
                if (n.obj.equals(obj))
                    iCount++;
            }
        }
        return iCount;
    }  
    
    void mapInsert(ISBEntry entry) {
        int h = entry.obj.hashCode();
        Set<ISBEntry> s;
        if (mapEntries.containsKey(h)) {
            s = mapEntries.get(h);
            s.add(entry);
        }
        else {
            s = new HashSet<>();
            s.add(entry);
            mapEntries.put(h, s);
        }
    }
    
    void mapDelete(ISBEntry entry) {
        int h = entry.obj.hashCode();
        if (mapEntries.containsKey(h)) {
            Set<ISBEntry> s = mapEntries.get(h);
            s.remove(entry);
            if (s.isEmpty()) { // ### added
                mapEntries.remove(h);
            }
        }
    }  
}
