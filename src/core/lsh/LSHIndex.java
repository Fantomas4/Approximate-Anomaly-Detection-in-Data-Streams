package core.lsh;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

public class LSHIndex {
    private final ArrayList<HashTable> hashTables;

    public LSHIndex(int numHashes, int numHashTables, int w, int dimensions) {
        // Create the collection of hash tables
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
        Set<Entry> indexQueryResults  = new HashSet<>();

        for (HashTable hashTable : hashTables) {
            ArrayList<Entry> tableQueryResults = hashTable.query(entry);
            if (tableQueryResults != null) {
                indexQueryResults.addAll(tableQueryResults);
            }
        }

        return new ArrayList<>(indexQueryResults);
    }
}
