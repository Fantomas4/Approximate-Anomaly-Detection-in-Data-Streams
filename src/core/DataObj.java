package core;


import core.lsh.Entry;

import javax.xml.crypto.Data;
import java.util.ArrayList;

public class DataObj<T> {

    protected double[] values;

    public StreamObj obj;
    public Long id;
    public int count_after;
    protected ArrayList<T> nn_before;

    // statistics
    public int nOutlier;
    public int nInlier;

    public DataObj(Long id, double[] values, StreamObj obj) {
        this.id = id;
        this.values = values;
        this.obj = obj;

        // init statistics
        nOutlier = 0;
        nInlier  = 0;
    }

//    public DataObj(Long id, StreamObj obj) {
//        this.id = id;
//        this.obj = obj;
//
//        // init statistics
//        nOutlier = 0;
//        nInlier  = 0;
//    }

    public DataObj(double[] values) {
        id = null;
        this.values = values;
        this.obj = null;

        // init statistics
        nOutlier = 0;
        nInlier  = 0;
    }

    public double dot (DataObj<T> other) {
        double sum = 0;

        for (int d = 0; d < values.length; d++) {
            sum += this.values[d] * other.values[d];
        }

        return sum;
    }
}
