/*
 *      _______                       _        ____ _     _
 *     |__   __|                     | |     / ____| |   | |
 *        | | __ _ _ __ ___  ___  ___| |    | (___ | |___| |
 *        | |/ _` | '__/ __|/ _ \/ __| |     \___ \|  ___  |
 *        | | (_| | |  \__ \ (_) \__ \ |____ ____) | |   | |
 *        |_|\__,_|_|  |___/\___/|___/_____/|_____/|_|   |_|
 *
 * -----------------------------------------------------------
 *
 *  TarsosLSH is developed by Joren Six.
 *
 * -----------------------------------------------------------
 *
 *  Info    : http://0110.be/tag/TarsosLSH
 *  Github  : https://github.com/JorenSix/TarsosLSH
 *  Releases: http://0110.be/releases/TarsosLSH/
 *
 */

package core.lsh;

import core.DataObj;
import core.StreamObj;

import java.util.*;

/**
 * An Entry contains a vector of 'dimension' values and an object. The object serves as the main data
 * structure that is stored and retrieved. It also has an identifier (key).
 *
 * @author Joren Six
 */
public class Entry extends DataObj<Entry> implements Comparable<Entry> {
    public static enum EntryType { OUTLIER, INLIER }

    public EntryType entryType;

    /**
     * An optional key, identifier for the entry.
     */
    private String key;

//    /**
//     * Creates a new entry with the requested number of dimensions.
//     * @param dimensions The number of dimensions.
//     */
//    public Entry(int dimensions) {
//        this(null,new double[dimensions]);
//    }
//
//
//    /**
//     * Copy constructor.
//     * @param other The other entry.
//     */
//    public Entry(Entry other){
//        //copy the values
//        this(other.getKey(),Arrays.copyOf(other.values, other.values.length));
//    }

    /**
     * Creates a new Entry with the requested number of dimensions.
     * @param dimensions The number of dimensions.
     */
    public Entry(int dimensions) {
        super(null, new double[dimensions], null);
        this.key = null;
    }

    public Entry(String key, Long id, double[] values, StreamObj obj){
        super(id, values, obj);
        this.key = key;

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
            Entry dummy = new Entry(null, sinceId, null, null);

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
            Entry dummy = new Entry(null, sinceId, null, null);
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






    /**
     * Moves the Entry's vector slightly, adds a value selected from -radius to +radius to each element.
     * @param radius The radius determines the amount to change the vector.
     */
    public void moveSlightly(double radius){
        Random rand = new Random();
        for (int d = 0; d < getDimensions(); d++) {
            //copy the point but add or subtract a value between -radius and +radius
            double diff = radius + (-radius - radius) * rand.nextDouble();
            double point = get(d) + diff;
            set(d, point);
        }
    }



    /**
     * Set a value at a certain dimension d.
     * @param dimension The dimension, index for the value.
     * @param value The value to set.
     */
    public void set(int dimension, double value) {
        values[dimension] = value;
    }

    /**
     * Returns the value at the requested dimension.
     * @param dimension The dimension, index for the value.
     * @return Returns the value at the requested dimension.
     */
    public double get(int dimension) {
        return values[dimension];
    }

    /**
     * @return The number of dimensions this vector has.
     */
    public int getDimensions(){
        return values.length;
    }

    /**
     * @return The data object of this entry.
     */
    public StreamObj getObj() {
        return obj;
    }

    /**
     * Calculates the dot product, or scalar product, of this entry's vector with the
     * other entry's vector.
     *
     * @param other
     *            The other vector, should have the same number of dimensions.
     * @return The dot product of this vector with the other vector.
     * @exception ArrayIndexOutOfBoundsException
     *                when the two vectors do not have the same dimensions.
     */
    public double dot(Entry other) {
        double sum = 0.0;
        for(int i=0; i < getDimensions(); i++) {
            sum += values[i] * other.values[i];
        }
        return sum;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public String toString(){
        StringBuilder sb= new StringBuilder();
        sb.append("values:[");
        for(int d=0; d < getDimensions() - 1; d++) {
            sb.append(values[d]).append(",");
        }
        sb.append(values[getDimensions()-1]).append("]");
        return sb.toString();
    }
}
