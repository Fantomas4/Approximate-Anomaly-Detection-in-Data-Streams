/*
 *    MCODBase.java
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

package algorithms;


import core.mcodbase.ISBIndex.ISBEntry;
import core.mcodbase.ISBIndex.ISBSearchResult;
import core.mcodbase.ISBIndex.ISBEntry.EntryType;
import core.mcodbase.MicroCluster;
import core.StreamObj;

import java.util.*;

public class ApproxMCOD extends MCODBase {
    // DIAG ONLY -- DELETE
    int diagExactMCCount = 0;
    int diagDiscardedMCCount = 0;
    int diagAdditionsToMC = 0;
    int diagAdditionsToPD = 0;
    int diagSafeInliersCount = 0;

    private double m_ar;
    private int m_pdLimit;
    private Set<ISBEntry> pdSafeInliers; // list of safe inliers

    private Random m_Random;

    public ApproxMCOD(int windowSize, int slideSize, double radius, int k, int pdLimit, double arFactor) {
        super(windowSize, slideSize, radius, k);

        m_Random = new Random();
        m_pdLimit = pdLimit;
        // create PD's safe inliers set
        pdSafeInliers = new HashSet<>();
        m_ar = (m_radius / 2.0) + arFactor * m_radius;

        System.out.println("Init ApproxMCOD:");
        System.out.println("   window_size: " + this.windowSize);
        System.out.println("   slide_size: " + this.slideSize);
        System.out.println("   radius: " + m_radius);
        System.out.println("   k: " + m_k);
        System.out.println("   PD Size Limit: " + m_pdLimit);
        System.out.println("   Approximation radius: " + m_ar);
    }

    ISBEntry getSafeInlier(int idx) {
        ISBEntry entry = null;
        Iterator it = pdSafeInliers.iterator();
        while (idx >= 0) {
            entry = (ISBEntry)it.next();
            idx--;
        }
        return entry;
    }

    void addNeighbor(ISBEntry entry, ISBEntry q, boolean bUpdateState) {
        // check if q still in window
        if (isElemInWindow(q.id) == false) {
            return;
        }

        if (getEntrySlide(q) >= getEntrySlide(entry)) {
            entry.count_after ++;
        } else {
            entry.addPrecNeigh(q);
        }
//        if (q.id < entry.id) {
//            entry.AddPrecNeigh(q);
//        } else {
//            entry.count_after++;
//        }

        if (bUpdateState) {
            // check if entry inlier or outlier
            int count = entry.count_after + entry.countPrecNeighs(windowStart);
            if ((entry.entryType == EntryType.OUTLIER) && (count >= m_k)) {
                // add entry to inlier set PD
                setEntryType(entry, EntryType.INLIER_PD);
                // If entry is an unsafe inlier, insert it to the event queue
                if (!isSafeInlier(entry)) {
                    ISBEntry entryMinExp = entry.getMinPrecNeigh(windowStart);
                    addToEventQueue(entry, entryMinExp);
                }
            }
        }
    }

    void processNewEntry(ISBEntry newEntry, boolean isNewEntry) {
        // Perform 3R/2 range query to cluster centers w.r.t new entry
        Vector<SearchResultMC> resultsMC;
        // results are sorted ascenting by distance
        resultsMC = RangeSearchMC(newEntry, 1.5 * m_radius);

        // Get closest micro-cluster
        MicroCluster mcClosest = null;
        if (resultsMC.size() > 0) {
            mcClosest = resultsMC.get(0).mc;
        }

        // check if newEntry can be inserted to closest micro-cluster
        boolean bFoundMC = false;
        if (mcClosest != null) {
            double d = GetEuclideanDist(newEntry, mcClosest.mcc);
            if (d <= m_radius / 2) {
                bFoundMC = true;
            }
        }

        if (bFoundMC) {
            // Add new entry to micro-cluster
            // DIAG ONLY -- DELETE
            diagAdditionsToMC++;
            newEntry.mc = mcClosest;
            setEntryType(newEntry, EntryType.INLIER_MC);
            mcClosest.addEntry(newEntry);

            // Update neighbors of set PD
            Vector<ISBEntry> entries;
            entries = ISB_PD.getAllEntries();
            for (ISBEntry q : entries) {
                if (q.Rmc.contains(mcClosest)) {
                    if (GetEuclideanDist(q, newEntry) <= m_radius) {
                        if (isNewEntry) {
                            // update q.count_after and its' outlierness
                            addNeighbor(q, newEntry, true);
                            // Add q to PD's safe inlier set if it is a safe inlier
                            if (isSafeInlier(q)) pdSafeInliers.add(q);
                        } else {
                            if (entriesReinsert.contains(q)) {
                                // update q.count_after or q.nn_before and its' outlierness
                                addNeighbor(q, newEntry, true);
                                // Add q to PD's safe inlier set if it is a safe inlier
                                if (isSafeInlier(q)) pdSafeInliers.add(q);
                            }
                        }
                    }
                }
            }
        }
        else {
            // Check ISB_PD's size to determine if a random safe inlier must be removed
            boolean safeInlierDeleted = false;
            int nSafeInliers = pdSafeInliers.size();
            while (ISB_PD.getSize() > m_pdLimit && nSafeInliers > 0) {
                int idx = m_Random.nextInt(nSafeInliers);
                ISBEntry si = getSafeInlier(idx);
                // Remove the selected safe inlier from the PD's ISB
                ISB_PD.remove(si);
                // Remove the selected safe inlier from the PD's safe inlier set
                pdSafeInliers.remove(si);
                // Update the value of the deletion flag
                safeInlierDeleted = true;
                // Update safe inliers counter
                nSafeInliers = pdSafeInliers.size();
            }

            // No close enough micro-cluster found.
            // Perform 3R/2 range query to entries in set PD.
            nRangeQueriesExecuted++;

            // create helper sets for micro-cluster management
            ArrayList<ISBEntry> setNC = new ArrayList<ISBEntry>();
            ArrayList<ISBEntry> setNNC = new ArrayList<ISBEntry>();
            ArrayList<ISBEntry> setANC = new ArrayList<ISBEntry>();
            Vector<ISBSearchResult> resultEntries;
            resultEntries = ISB_PD.RangeSearch(newEntry, 1.5 * m_radius); // 1.5 ###
            for (ISBSearchResult sr : resultEntries) {
                ISBEntry q = sr.entry;
                if (sr.distance <= m_radius) {
                    // add q to neighs of newEntry
                    addNeighbor(newEntry, q, false);
                    if (isNewEntry) {
                        // update q.count_after and its' outlierness
                        addNeighbor(q, newEntry, true);
                        // Add q to PD's safe inlier set if it is a safe inlier
                        if (isSafeInlier(q)) pdSafeInliers.add(q);
                    } else {
                        if (entriesReinsert.contains(q)) {
                            // update q.count_after or q.nn_before and its' outlierness
                            addNeighbor(q, newEntry, true);
                            // Add q to PD's safe inlier set if it is a safe inlier
                            if (isSafeInlier(q)) pdSafeInliers.add(q);
                        }
                    }
                }

                if (sr.distance <= m_radius / 2.0) {
                    setNC.add(q);
                } else {
                    setNNC.add(q);
                    if (safeInlierDeleted && sr.distance <= m_ar) {
                        setANC.add(q);
                    }
                }
            }

            // check if the number of objects found is big enough to create a cluster
            int nCollectedObjects;
            if (safeInlierDeleted) {
                // Check size of sets NC and ANC
                nCollectedObjects = setNC.size() + setANC.size();
            } else {
                // Check size of set NC
                nCollectedObjects = setNC.size();
            }
            if (nCollectedObjects >= m_theta * m_k) {
                // DIAG ONLY -- DELETE
                diagExactMCCount ++;

                // create new micro-cluster with center newEntry
                MicroCluster mcNew = new MicroCluster(newEntry);
                addMicroCluster(mcNew);
                newEntry.mc = mcNew;
                // DIAG ONLY -- DELETE
                diagAdditionsToMC++;
                setEntryType(newEntry, EntryType.INLIER_MC);

                // Add to new mc entries within range R/2
                for (ISBEntry q : setNC) {
                    q.mc = mcNew;
                    // DIAG ONLY -- DELETE
                    diagAdditionsToMC++;
                    mcNew.addEntry(q);
                    // move q from set PD to set inlier-mc
                    setEntryType(q, EntryType.INLIER_MC);
                    ISB_PD.remove(q);
                    // If q is a safe inlier, also remove it from the PD's safe inlier set.
                    if (isSafeInlier(q)) pdSafeInliers.remove(q);
                }
                // Add to new mc entries within range ar
                for (ISBEntry q : setANC) {
                    q.mc = mcNew;
                    // DIAG ONLY -- DELETE
                    diagAdditionsToMC++;
                    mcNew.addEntry(q);
                    // move q from set PD to set inlier-mc
                    setEntryType(q, EntryType.INLIER_MC);
                    ISB_PD.remove(q);
                    // If q is a safe inlier, also remove it from the PD's safe inlier set.
                    if (isSafeInlier(q)) pdSafeInliers.remove(q);
                }


                // Update Rmc lists of entries of PD in range 3R/2 from mcNew
                for (ISBEntry q : setNNC) {
                    q.Rmc.add(mcNew);
                }
            } else {
                // Add to newEntry neighs entries of near micro-clusters
                for (SearchResultMC sr : resultsMC) {
                    for (ISBEntry q : sr.mc.entries) {
                        if (GetEuclideanDist(q, newEntry) <= m_radius) {
                            // add q to neighs of newEntry
                            addNeighbor(newEntry, q, false);
                        }
                    }
                }

                // check if newEntry is an inlier or outlier
                // use both nn_before and count_after for case isNewEntry=false
                int count = newEntry.countPrecNeighs(windowStart) + newEntry.count_after;
                if (count >= m_k) {
                    // newEntry is an inlier
                    setEntryType(newEntry, EntryType.INLIER_PD);
                    // If newEntry is an unsafe inlier, insert it to the event queue
                    if (!isSafeInlier(newEntry)) {
                        ISBEntry entryMinExp = newEntry.getMinPrecNeigh(windowStart);
                        addToEventQueue(newEntry, entryMinExp);
                    }
                } else {
                    // newEntry is an outlier
                    setEntryType(newEntry, EntryType.OUTLIER);
                }

                // If newEntry is not a safe inlier, add it to PD
                // If newEntry is a safe inlier, add it to PD only if PD size limit has not been reached.
                if (!isSafeInlier(newEntry) || (isSafeInlier(newEntry) && ISB_PD.getSize() < m_pdLimit)) {
                    // Insert newEntry to index of entries of PD
                    ISB_PD.insert(newEntry);
                    diagAdditionsToPD++;

                    // Update newEntry.Rmc
                    for (SearchResultMC sr : resultsMC) {
                        newEntry.Rmc.add(sr.mc);
                    }
                }
            }
        }
    }

    void ProcessEventQueue(ISBEntry entryExpired) {
        // DIAG ONLY -- DELETE
        diagSafeInliersCount = 0;

        EventItem e = eventQueue.findMin();
        while ((e != null) && (e.timeStamp <= windowEnd)) {
            e = eventQueue.extractMin();
            ISBEntry x = e.entry;
            // entry x must be in window and not in any micro-cluster
            boolean bValid = ( isElemInWindow(x.id) && (x.mc == null) );
            if (bValid) {
                // remove entryExpired from x.nn_before
                x.removePrecNeigh(entryExpired);
                // get amount of neighbors of x
                int count = x.count_after + x.countPrecNeighs(windowStart);
                if (count < m_k) {
                    // x is an outlier
                    setEntryType(x, EntryType.OUTLIER);
                } else {
                    // DIAG ONLY -- DELETE
                    if (x.count_after >= m_k) diagSafeInliersCount++;

                    // If x is an unsafe inlier, add it to the event queue
                    if (!isSafeInlier(x)) {
                        // get oldest preceding neighbor of x
                        ISBEntry entryMinExp = x.getMinPrecNeigh(windowStart);
                        // add x to event queue
                        addToEventQueue(x, entryMinExp);
                    }
                }
            }
            e = eventQueue.findMin();
        }
    }

    void processExpiredEntries(ArrayList<ISBEntry> expiredEntries) {
        for (ISBEntry expiredEntry : expiredEntries) {
            MicroCluster mc = expiredEntry.mc;
            if (mc != null) {
                mc.removeEntry(expiredEntry);
                if (mc.getEntriesCount() < m_k + 1) {
                    // DIAG ONLY -- DELETE
                    diagDiscardedMCCount ++;

                    // remove micro-cluster mc
                    try {
                        RemoveMicroCluster(mc);
                    } catch (CorruptedDataStateException e) {
                        e.printStackTrace();
                    }

                    // insert entries of mc to set entriesReinsert
                    entriesReinsert = new TreeSet<ISBEntry>();
                    for (ISBEntry q : mc.entries) {
                        entriesReinsert.add(q);
                    }

                    // treat each entry of mc as new entry
                    for (ISBEntry q : mc.entries) {
                        q.initEntry();
                        processNewEntry(q, false);
                    }
                }
            } else {
                // expiredEntry belongs to set PD
                // remove expiredEntry from PD index
                ISB_PD.remove(expiredEntry);
            }

            removeEntry(expiredEntry);
            ProcessEventQueue(expiredEntry);
        }
    }

    public void ProcessNewStreamObjects(ArrayList<StreamObj> streamObjs) {
        if (windowElements.size() >= windowSize) {
            // If the window is full, perform a slide
            doSlide();
            // Process expired entries
            processExpiredEntries(getExpiredEntries());
        }

        // Process new entries
        for (StreamObj streamObj : streamObjs) {
            ISBEntry newEntry = new ISBEntry(streamObj, objId); // create new ISB entry
            addEntry(newEntry); // add newEntry to window
            processNewEntry(newEntry, true);

            objId++; // update object identifier
        }


        // DIAG ONLY -- DELETE
        System.out.println("---------------------- ApproxMCOD ----------------------");
        System.out.println("DIAG - Current stream object: " + (objId - 1));
        System.out.println("DIAG - Total Exact MCs count: " + diagExactMCCount);
        System.out.println("DIAG - Total Discarded MCs: " + diagDiscardedMCCount);
//        System.out.println("DIAG - #Times an MC was sustained: " + diagSustainedMCCount);
        System.out.println("DIAG - #Times a point was added to an MC: " + diagAdditionsToMC);
        System.out.println("DIAG - #Times a point was added to PD: " + diagAdditionsToPD);
//        System.out.println("DIAG - #Safe inliers detected: " + diagSafeInliersCount);
        System.out.println("DIAG - Total -ACTIVE- MCs: " + setMC.size());
        System.out.println("DIAG - Total -ACTIVE- PD's Safe Inliers List Population: " + pdSafeInliers.size());
        System.out.println("DIAG - Total -ACTIVE- PD List Population: " + ISB_PD.getSize());
        System.out.println("DIAG - TEMP OUTLIER SET SIZE: " + GetOutliersFound().size());
        System.out.println("DIAG - TEMP Window size is: " + windowElements.size());
        System.out.println("--------------------------------------------------------");
    }

    private ArrayList<ISBEntry> getExpiredEntries() {
        ArrayList<ISBEntry> expiredEntries = new ArrayList<>();
        for (ISBEntry entry : windowElements) {
            // check if entry has expired
            if (entry.id < windowStart) {
                expiredEntries.add(entry);
            } else {
                break;
            }
        }
        return expiredEntries;
    }
}
