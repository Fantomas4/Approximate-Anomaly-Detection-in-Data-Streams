/*
 *    MicroCluster.java
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


import core.mtree.DistanceFunctions;
import core.mcodbase.ISBIndex.ISBEntry;
import java.util.ArrayList;

public class MicroCluster implements DistanceFunctions.EuclideanCoordinate, Comparable<MicroCluster> {
    public ISBEntry mcc;
    public ArrayList<ISBEntry> entries;

    public MicroCluster(ISBEntry mcc) {
        this.mcc = mcc;
        entries = new ArrayList<>();
        addEntry(mcc);
    }
    
    public void addEntry(ISBEntry entry) {
        if (entry != null)
            entries.add(entry);
    }
    
    public void removeEntry(ISBEntry entry) {
        if (entry != null)
            entries.remove(entry);
    }
    
    public int getEntriesCount() {
        return entries.size();
    }

    @Override
    public int dimensions() {
        return mcc.obj.dimensions();
    }

    @Override
    public double get(int index) {
        return mcc.obj.get(index);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof MicroCluster) {
            MicroCluster that = (MicroCluster) obj;
            if (this.dimensions() != that.dimensions()) {
                return false;
            }
            for (int i = 0; i < this.dimensions(); i++) {
                if (this.mcc.obj.get(i) != that.mcc.obj.get(i)) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int compareTo(MicroCluster that) {
        int dimensions = Math.min(this.dimensions(), that.dimensions());
        for (int i = 0; i < dimensions; i++) {
            double v1 = this.mcc.obj.get(i);
            double v2 = that.mcc.obj.get(i);
            if (v1 > v2) {
                return +1;
            }
            if (v1 < v2) {
                return -1;
            }
        }

        if (this.dimensions() > dimensions) {
            return +1;
        }

        if (that.dimensions() > dimensions) {
            return -1;
        }

        return 0;
    }
}