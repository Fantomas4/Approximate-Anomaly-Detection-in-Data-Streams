package core.lsh;

import java.util.*;

public class HashTable {
    private final HashMap<String, ArrayList<Entry>> hashTable;
    private final int numHashes;
    private final HashFunction[] hashFunctions;

    public HashTable(int numHashes, int w, int dimensions) {
        this.numHashes = numHashes;

        hashTable = new HashMap<>();
        // Generate the hash functions of the hash table
        hashFunctions = new HashFunction[numHashes];
        for (int i = 0; i < numHashes; i++) {
            hashFunctions[i] = new HashFunction(dimensions, w);
        }
    }

    public void add(Entry entry) {
        String combinedHash = generateCombinedHash(entry);

        if (hashTable.containsKey(combinedHash)) {
            hashTable.get(combinedHash).add(entry);
        } else {
            ArrayList<Entry> newBucket = new ArrayList<>();
            newBucket.add(entry);
            hashTable.put(combinedHash, newBucket);
        }
    }

    public void remove(Entry entry) {
        String combinedHash = generateCombinedHash(entry);
        hashTable.get(combinedHash).remove(entry);
    }

    public ArrayList<Entry> query(Entry entry) {
        String combinedHash = generateCombinedHash(entry);
        return hashTable.get(combinedHash);
    }

    private String generateCombinedHash(Entry entry) {
        int[] individualHashes = new int[numHashes];

        for (int f = 0; f < numHashes; f++) {
            individualHashes[f] = hashFunctions[f].hash(entry);
        }

        // Combine the individual hashes into one
        return Arrays.toString(individualHashes);
    }
}
