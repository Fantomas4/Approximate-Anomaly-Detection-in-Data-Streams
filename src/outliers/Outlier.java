package outliers;

import algorithms.ISBIndex.ISBNode;

public class Outlier implements Comparable<Outlier> {
    public long id;
    public Object obj;

    public Outlier(ISBNode node) {
        this.id = node.id;
        this.obj = node;
    }

    @Override
    public int compareTo(Outlier o) {
        if (this.id > o.id)
            return 1;
        else if (this.id < o.id)
            return -1;
        else
            return 0;
    }

    @Override
    public boolean equals(Object o) {
        return (this.id == ((Outlier) o).id);
    }
}