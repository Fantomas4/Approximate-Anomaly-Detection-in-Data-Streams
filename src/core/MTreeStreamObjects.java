/*
 *    MTreeStreamObjects.java
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

package core;

import core.mtree.*;
import core.mtree.utils.Pair;
import core.mtree.utils.Utils;
import java.util.Set;

class MTreeStreamObjects extends MTree<StreamObj> {

    private static final PromotionFunction<StreamObj> nonRandomPromotion = new PromotionFunction<StreamObj>() {

        @Override
        public Pair<StreamObj> process(Set<StreamObj> dataSet, DistanceFunction<? super StreamObj> distanceFunction) {
            return Utils.minMax(dataSet);
        }
    };

    MTreeStreamObjects() {
        super(25, DistanceFunctions.EUCLIDEAN,
                new ComposedSplitFunction<StreamObj>(
                nonRandomPromotion,
                new PartitionFunctions.BalancedPartition<StreamObj>()));
    }

    public void add(StreamObj data) {
        super.add(data);
        _check();
    }

    public boolean remove(StreamObj data) {
        boolean result = super.remove(data);
        _check();
        return result;
    }

    DistanceFunction<? super StreamObj> getDistanceFunction() {
        return distanceFunction;
    }
};
