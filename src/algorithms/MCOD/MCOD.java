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

package algorithms.MCOD;


import algorithms.ISBIndex;
import algorithms.ISBIndex.ISBNode;
import algorithms.ISBIndex.ISBSearchResult;
import algorithms.ISBIndex.ISBNode.NodeType;
import algorithms.MTreeMicroClusters;
import algorithms.MicroCluster;

import java.util.*;

public abstract class MCOD {
    protected static class EventItem implements Comparable<EventItem> {
        public ISBNode node;
        public Long timeStamp;

        public EventItem(ISBNode node, Long timeStamp) {
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
        
        public void Insert(ISBNode node, Long expTime) {
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
    protected TreeSet<ISBNode> nodesReinsert;
    // index of objects not in any micro-cluster
    protected ISBIndex ISB_PD;
    protected int m_WindowSize;
    protected double m_radius;
    protected int m_k;
    protected double m_theta = 1.0;

    // statistics
    public int m_nBothInlierOutlier;
    public int m_nOnlyInlier;
    public int m_nOnlyOutlier;

    // DIAG ONLY -- DELETE
    int diagExactMCCount = 0;
    int diagDiscardedMCCount = 0;
    int diagAdditionsToMC = 0;
    int diagAdditionsToPD = 0;
    int diagSafeInliersCount = 0;

    public MCOD(int windowSize, double radius, int k) {
        m_WindowSize = windowSize;
        m_radius = radius;
        m_k = k;

        System.out.println("Init MCOD:");
        System.out.println("   window_size: " + m_WindowSize);
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

    void SetNodeType(ISBNode node, NodeType type) {
        node.nodeType = type;
        // update statistics
        if (type == NodeType.OUTLIER)
            node.nOutlier++;
        else
            node.nInlier++;
    }

    void AddNeighbor(ISBNode node, ISBNode q, boolean bUpdateState) {
        // check if q still in window
        if (IsNodeIdInWin(q.id) == false) {
            return;
        }

        if (q.id < node.id) {
            node.AddPrecNeigh(q);
        } else {
            node.count_after++;
        }

        if (bUpdateState) {
            // check if node inlier or outlier
            int count = node.count_after + node.CountPrecNeighs(GetWindowStart());
            if ((node.nodeType == NodeType.OUTLIER) && (count >= m_k)) {
                // remove node from outliers
                RemoveOutlier(node);
                // add node to inlier set PD
                SetNodeType(node, NodeType.INLIER_PD);
                // insert node to event queue
                ISBNode nodeMinExp = node.GetMinPrecNeigh(GetWindowStart());
                AddToEventQueue(node, nodeMinExp);
            }
        }
    }

    void AddToEventQueue(ISBNode x, ISBNode nodeMinExp) {
        if (nodeMinExp != null) {
            Long expTime = GetExpirationTime(nodeMinExp);
            eventQueue.Insert(x, expTime);
        }
    }

    void ProcessNewNode(ISBNode nodeNew, boolean bNewNode) {
        // Perform 3R/2 range query to cluster centers w.r.t new node
        Vector<SearchResultMC> resultsMC;
        // results are sorted ascenting by distance
        resultsMC = RangeSearchMC(nodeNew, 1.5 * m_radius);

        // Get closest micro-cluster
        MicroCluster mcClosest = null;
        if (resultsMC.size() > 0) {
            mcClosest = resultsMC.get(0).mc;
        }

        // check if nodeNew can be inserted to closest micro-cluster
        boolean bFoundMC = false;
        if (mcClosest != null) {
            double d = GetEuclideanDist(nodeNew, mcClosest.mcc);
            if (d <= m_radius / 2) {
                bFoundMC = true;
            }
        }

        if (bFoundMC) {
            // Add new node to micro-cluster
            // DIAG ONLY -- DELETE
            diagAdditionsToMC++;
            nodeNew.mc = mcClosest;
            SetNodeType(nodeNew, NodeType.INLIER_MC);
            mcClosest.AddNode(nodeNew);

            // Update neighbors of set PD
            Vector<ISBNode> nodes;
            nodes = ISB_PD.GetAllNodes();
            for (ISBNode q : nodes) {
                if (q.Rmc.contains(mcClosest)) {
                    if (GetEuclideanDist(q, nodeNew) <= m_radius) {
                        if (bNewNode) {
                            // update q.count_after and its' outlierness
                            AddNeighbor(q, nodeNew, true);
                        } else {
                            if (nodesReinsert.contains(q)) {
                                // update q.count_after or q.nn_before and its' outlierness
                                AddNeighbor(q, nodeNew, true);
                            }
                        }
                    }
                }
            }
        }
        else {
            // No close enough micro-cluster found.
            // Perform 3R/2 range query to nodes in set PD.
            nRangeQueriesExecuted++;
            // create helper sets for micro-cluster management
            ArrayList<ISBNode> setNC = new ArrayList<ISBNode>();
            ArrayList<ISBNode> setNNC = new ArrayList<ISBNode>();
            Vector<ISBSearchResult> resultNodes;
            resultNodes = ISB_PD.RangeSearch(nodeNew, 1.5 * m_radius); // 1.5 ###
            for (ISBSearchResult sr : resultNodes) {
                ISBNode q = sr.node;
                if (sr.distance <= m_radius) {
                    // add q to neighs of nodeNew
                    AddNeighbor(nodeNew, q, false);
                    if (bNewNode) {
                        // update q.count_after and its' outlierness
                        AddNeighbor(q, nodeNew, true);
                    } else {
                        if (nodesReinsert.contains(q)) {
                            // update q.count_after or q.nn_before and its' outlierness
                            AddNeighbor(q, nodeNew, true);
                        }
                    }
                }

                if (sr.distance <= m_radius / 2.0) {
                    setNC.add(q);
                } else {
                    setNNC.add(q);
                }
            }

            // check if size of set NC big enough to create cluster
            if (setNC.size() >= m_theta * m_k) {
                // DIAG ONLY -- DELETE
                diagExactMCCount ++;

                // create new micro-cluster with center nodeNew
                MicroCluster mcNew = new MicroCluster(nodeNew);
                AddMicroCluster(mcNew);
                nodeNew.mc = mcNew;
                // DIAG ONLY -- DELETE
                diagAdditionsToMC++;
                SetNodeType(nodeNew, NodeType.INLIER_MC);

                // Add to new mc nodes within range R/2
                for (ISBNode q : setNC) {
                    q.mc = mcNew;
                    // DIAG ONLY -- DELETE
                    diagAdditionsToMC++;
                    mcNew.AddNode(q);
                    // move q from set PD to set inlier-mc
                    SetNodeType(q, NodeType.INLIER_MC);
                    ISB_PD.Remove(q);
                    RemoveOutlier(q); // needed? ###
                }

                // Update Rmc lists of nodes of PD in range 3R/2 from mcNew
                for (ISBNode q : setNNC) {
                    q.Rmc.add(mcNew);
                }
            } else {
                // Add to nodeNew neighs nodes of near micro-clusters
                for (SearchResultMC sr : resultsMC) {
                    for (ISBNode q : sr.mc.nodes) {
                        if (GetEuclideanDist(q, nodeNew) <= m_radius) {
                            // add q to neighs of nodeNew
                            AddNeighbor(nodeNew, q, false);
                        }
                    }
                }

                ISB_PD.Insert(nodeNew);
                diagAdditionsToPD++;

                // check if nodeNew is an inlier or outlier
                // use both nn_before and count_after for case bNewNode=false
                int count = nodeNew.CountPrecNeighs(GetWindowStart()) + nodeNew.count_after;
                if (count >= m_k) {
                    // nodeNew is an inlier
                    SetNodeType(nodeNew, NodeType.INLIER_PD);
                    // insert nodeNew to event queue
                    ISBNode nodeMinExp = nodeNew.GetMinPrecNeigh(GetWindowStart());
                    AddToEventQueue(nodeNew, nodeMinExp);
                } else {
                    // nodeNew is an outlier
                    SetNodeType(nodeNew, NodeType.OUTLIER);
                    SaveOutlier(nodeNew);
                }

                // Update nodeNew.Rmc
                for (SearchResultMC sr : resultsMC) {
                    nodeNew.Rmc.add(sr.mc);
                }
            }
        }
    }

    void ProcessEventQueue(ISBNode nodeExpired) {
        // DIAG ONLY -- DELETE
        diagSafeInliersCount = 0;

        EventItem e = eventQueue.FindMin();
        while ((e != null) && (e.timeStamp <= GetWindowEnd())) {
            e = eventQueue.ExtractMin();
            ISBNode x = e.node;
            // node x must be in window and not in any micro-cluster
            boolean bValid = ( IsNodeIdInWin(x.id) && (x.mc == null) );
            if (bValid) {
                // remove nodeExpired from x.nn_before
                x.RemovePrecNeigh(nodeExpired);
                // get amount of neighbors of x
                int count = x.count_after + x.CountPrecNeighs(GetWindowStart());
                if (count < m_k) {
                    // x is an outlier
                    SetNodeType(x, NodeType.OUTLIER);
                    SaveOutlier(x);
                } else {
                    // x is an inlier, add to event queue
                    // DIAG ONLY -- DELETE
                    if (x.count_after >= m_k) diagSafeInliersCount++;

                    // get oldest preceding neighbor of x
                    ISBNode nodeMinExp = x.GetMinPrecNeigh(GetWindowStart());
                    // add x to event queue
                    AddToEventQueue(x, nodeMinExp);
                }
            }
            e = eventQueue.FindMin();
        }
    }

    void ProcessExpiredNode(ISBNode nodeExpired) {
        if (nodeExpired != null) {
            MicroCluster mc = nodeExpired.mc;
            if (mc != null) {
                mc.RemoveNode(nodeExpired);
                if (mc.GetNodesCount() < m_k) {
                    // DIAG ONLY -- DELETE
                    diagDiscardedMCCount ++;

                    // remove micro-cluster mc
                    RemoveMicroCluster(mc);

                    // insert nodes of mc to set nodesReinsert
                    nodesReinsert = new TreeSet<ISBNode>();
                    for (ISBNode q : mc.nodes) {
                        nodesReinsert.add(q);
                    }

                    // treat each node of mc as new node
                    for (ISBNode q : mc.nodes) {
                        q.InitNode();
                        ProcessNewNode(q, false);
                    }
                }
            } else {
                // nodeExpired belongs to set PD
                // remove nodeExpired from PD index
                ISB_PD.Remove(nodeExpired);
            }

            RemoveNode(nodeExpired);
            ProcessEventQueue(nodeExpired);
        }
    }

    protected void ProcessNewStreamObj(Instance inst)
    {
        if (bShowProgress) ShowProgress("Processed " + (objId-1) + " stream objects.");
        // PrintInstance(inst);

        double[] values = getInstanceValues(inst);
        StreamObj obj = new StreamObj(values);

        if (bTrace) Println("\n- - - - - - - - - - - -\n");

        // create new ISB node
        ISBNode nodeNew = new ISBNode(inst, obj, objId);
        if (bTrace) { Print("New node: "); PrintNode(nodeNew); }

        objId++; // update object identifier (slide window)

        AddNode(nodeNew); // add nodeNew to window
        if (bTrace) PrintWindow();

        ProcessNewNode(nodeNew, true);
        ProcessExpiredNode(GetExpiredNode());

        if (bTrace) {
            Print("Micro-clusters: "); PrintMCSet(setMC);
            PrintOutliers();
            PrintPD();
        }
        // DIAG ONLY -- DELETE
        System.out.println("-------------------- MCOD baseline --------------------");
        System.out.println("DIAG - Total Exact MCs count: " + diagExactMCCount);
        System.out.println("DIAG - Total Discarded MCs: " + diagDiscardedMCCount);
        System.out.println("DIAG - #Times a point was added to an MC: " + diagAdditionsToMC);
        System.out.println("DIAG - #Times a point was added to PD: " + diagAdditionsToPD);
        System.out.println("DIAG - #Safe inliers detected: " + diagSafeInliersCount);
        System.out.println("DIAG - Total -ACTIVE- MCs: " + setMC.size());
        System.out.println("DIAG - Total -ACTIVE- PD List Population: " + ISB_PD.GetSize());
        System.out.println("DIAG - Process time (until now): " + nTotalRunTime / 1000.0);
        System.out.println("-------------------------------------------------------");
    }







    public String getObjectInfo(Object obj) {
        if (obj == null) return null;
        
        ISBNode node = (ISBNode) obj;
        
        ArrayList<String> infoTitle = new ArrayList<String>();
        ArrayList<String> infoValue = new ArrayList<String>();
        StringBuilder sb = new StringBuilder();
        
        // show node type
        infoTitle.add("Node type");
        infoValue.add((node.nodeType == NodeType.OUTLIER) ? "Outlier" : "Inlier");

        // show node position
        for (int i = 0; i < node.obj.dimensions(); i++) {
            infoTitle.add("Dim" + (i+1));
            infoValue.add(String.format("%.3f", node.obj.get(i)));
        }
        
        // show node properties
        infoTitle.add("id");
        infoValue.add(String.format("%d", node.id));
        infoTitle.add("count_after");
        infoValue.add(String.format("%d", node.count_after));
        infoTitle.add("|nn_before|");
        infoValue.add(String.format("%d", node.CountPrecNeighs(GetWindowStart())));
        
        sb.append("<html>");
        sb.append("<table>");
        int i = 0;
        while(i < infoTitle.size() && i < infoValue.size()){
            sb.append("<tr><td><b>"+infoTitle.get(i)+":</b></td><td>"+infoValue.get(i)+"</td></tr>");
            i++;
        }
        sb.append("</table>");

        
        sb.append("</html>");
        return sb.toString();
    }
    
    public String getStatistics() {
        StringBuilder sb = new StringBuilder();
        
        sb.append("Statistics:\n\n");
        
        // get counters of expired nodes
        int nBothInlierOutlier = m_nBothInlierOutlier;
        int nOnlyInlier = m_nOnlyInlier;
        int nOnlyOutlier = m_nOnlyOutlier;
        
        // add counters of non expired nodes
        for (ISBNode node : windowNodes) {
            if ((node.nInlier > 0) && (node.nOutlier > 0))
                nBothInlierOutlier++;
            else if (node.nInlier > 0)
                nOnlyInlier++;
            else
                nOnlyOutlier++;
        }
        
        int sum = nBothInlierOutlier + nOnlyInlier + nOnlyOutlier;
        if (sum > 0) {
            sb.append(String.format("  Nodes always inlier: %d (%.1f%%)\n", nOnlyInlier, (100 * nOnlyInlier) / (double)sum));
            sb.append(String.format("  Nodes always outlier: %d (%.1f%%)\n", nOnlyOutlier, (100 * nOnlyOutlier) / (double)sum));
            sb.append(String.format("  Nodes both inlier and outlier: %d (%.1f%%)\n", nBothInlierOutlier, (100 * nBothInlierOutlier) / (double)sum));
            
            sb.append("  (Sum: " + sum + ")\n");
        }
        
        sb.append("\n  Total range queries: " + nRangeQueriesExecuted + "\n");
        sb.append("  Max memory usage: " + iMaxMemUsage + " MB\n");
        sb.append("  Total process time: " + String.format("%.2f ms", nTotalRunTime / 1000.0) + "\n");
        
        return sb.toString();
    }
    
    Long GetWindowEnd() {
        return objId - 1;
    }
    
    Long GetWindowStart() {
        Long x = GetWindowEnd() - m_WindowSize + 1;
        if (x < FIRST_OBJ_ID) 
            x = FIRST_OBJ_ID;
        return x;
    }
    
    boolean IsWinFull() {
        return (GetWindowEnd() >= FIRST_OBJ_ID + m_WindowSize - 1);
    }
    
    Long GetExpirationTime(ISBNode node) {
        return node.id + m_WindowSize;
    }
    
    void SaveOutlier(ISBNode node) {
        AddOutlier(new Outlier(node.inst, node.id, node));
        node.nOutlier++; // update statistics
    }
    
    void RemoveOutlier(ISBNode node) {
        RemoveOutlier(new Outlier(node.inst, node.id, node));
        node.nInlier++; // update statistics
    }
    
    protected boolean IsNodeIdInWin(long id) {
        int toleranceStart = 1;
        Long start = GetWindowStart() - toleranceStart;
        if ( (start <= id) && (id <= GetWindowEnd()) )
            return true;
        else
            return false;
    }
    
    void AddNode(ISBNode node) {
        windowNodes.add(node);
    }
    
    void RemoveNode(ISBNode node) {
        windowNodes.remove(node);
        // update statistics
        UpdateStatistics(node);
    }
    
    void UpdateStatistics(ISBNode node) {
        if ((node.nInlier > 0) && (node.nOutlier > 0))
            m_nBothInlierOutlier++;
        else if (node.nInlier > 0)
            m_nOnlyInlier++;
        else
            m_nOnlyOutlier++;
    }
    
    ISBNode GetExpiredNode() {
        if (windowNodes.size() <= 0)
            return null;       
        // get oldest node
        ISBNode node = windowNodes.get(0);
        // check if node has expired
        if (node.id < GetWindowStart()) {
            return node;
        }        
        return null;
    }
    
    void AddMicroCluster(MicroCluster mc) {
        mtreeMC.add(mc);
        setMC.add(mc);
    }
    
    void RemoveMicroCluster(MicroCluster mc) {
        mtreeMC.remove(mc);
        setMC.remove(mc);
    }
    
    class SearchResultMC {
        MicroCluster mc;
        double distance;

        public SearchResultMC(MicroCluster mc, double distance) {
            this.mc = mc;
            this.distance = distance;
        }
    }
    
    Vector<SearchResultMC> RangeSearchMC(ISBNode nodeNew, double radius) {
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
    
    double GetEuclideanDist(ISBNode n1, ISBNode n2)
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

}
