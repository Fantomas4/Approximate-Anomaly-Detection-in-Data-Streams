package core.lsh;

import java.lang.reflect.Array;
import java.util.ArrayList;

public class LSHIndex {
    private final ArrayList<HashTable> hashTables;

    public LSHIndex(int numHashes, int numHashTables, int w, int dimensions) {
        // Generate the collection of hash tables using unique HashFunctio
        hashTables = new ArrayList<>();
        for (int t = 0; t < numHashTables; t++) {
            hashTables.add(new HashTable(numHashes, w, dimensions));
        }
    }

    public void add(Entry entry) {
        for (HashTable hashTable : hashTables) {
            hashTable.add(entry);
        }
    }

    public void remove(Entry entry) {
        for (HashTable hashTable : hashTables) {
            hashTable.remove(entry);
        }
    }

    public ArrayList<Entry> query(Entry entry) {
        ArrayList<Entry> queryResults = new ArrayList<>();

        for (HashTable hashTable : hashTables) {
            queryResults.addAll(hashTable.query(entry));
        }

        return queryResults;
    }
}
