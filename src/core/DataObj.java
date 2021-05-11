package core;


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

    public DataObj(Long id, StreamObj obj) {
        this.id = id;
        this.obj = obj;

        // init statistics
        nOutlier = 0;
        nInlier  = 0;
    }
}
