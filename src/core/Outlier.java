package core;

public class Outlier<T extends DataObj<?>> implements Comparable<Outlier> {
    public long id;
    public Object obj;

    public Outlier(T elem) {
        this.id = elem.id;
        this.obj = elem;
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