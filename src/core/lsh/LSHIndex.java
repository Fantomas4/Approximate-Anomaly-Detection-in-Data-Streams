package core.lsh;

import core.DataObj;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class LSHIndex<T extends DataObj<T>> {
    private final ArrayList<HashTable<T>> hashTables;

    public LSHIndex(int numHashes, int numHashTables, int w, int dimensions) {
        // Create the collection of hash tables
        hashTables = new ArrayList<>();
        for (int t = 0; t < numHashTables; t++) {
            hashTables.add(new HashTable<T>(numHashes, w, dimensions));
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

    // Returns the total amount of entries stored in the LSH Index
    public int getSize() {
        int sum = 0;

        for (HashTable<T> hashTable : hashTables) {
            sum += hashTable.getSize();
        }

        return sum;
    }

    public ArrayList<T> getAllEntries() {
        ArrayList<T> allEntries = new ArrayList<>();

        for (HashTable<T> hashTable : hashTables) {
            allEntries.addAll(hashTable.getAllEntries());
        }

        return allEntries;
    }
}
