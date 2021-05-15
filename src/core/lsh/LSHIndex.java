package core.lsh;

import core.DataObj;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class LSHIndex<T extends DataObj<T>> {
    private final ArrayList<HashTable<T>> hashTables;

    public LSHIndex(int numHashes, int numHashTables, int w, int dimensions, int k) {
        // Create the collection of hash tables
        hashTables = new ArrayList<>();
        for (int t = 0; t < numHashTables; t++) {
            hashTables.add(new HashTable<T>(numHashes, w, dimensions, k));
        }
    }

    public void insert(T entry) {
        for (HashTable<T> hashTable : hashTables) {
            hashTable.add(entry);
        }
    }

    public void remove(T entry) {
        for (HashTable<T> hashTable : hashTables) {
            hashTable.remove(entry);
        }
    }

    public ArrayList<T> query(T entry) {
        Set<T> indexQueryResults  = new HashSet<>();

        for (HashTable<T> hashTable : hashTables) {
            ArrayList<T> tableQueryResults = hashTable.query(entry);
            if (tableQueryResults != null) {
                indexQueryResults.addAll(tableQueryResults);
            }
        }

        return new ArrayList<>(indexQueryResults);
    }

    public ArrayList<T> getAllEntries() {
        HashSet<T> uniqueEntries = new HashSet<>();

        for (HashTable<T> hashTable : hashTables) {
            uniqueEntries.addAll(hashTable.getAllEntries());
        }

        return new ArrayList<>(uniqueEntries);
    }

//    // Returns the total amount of entries stored in the LSH Index
//    public int getSize() {
//        return getAllEntries().size();
//    }

}
