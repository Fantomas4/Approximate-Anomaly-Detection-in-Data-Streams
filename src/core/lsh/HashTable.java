package core.lsh;

import core.DataObj;

import java.lang.reflect.Array;
import java.util.*;

public class HashTable<T extends DataObj<T>> {
    private final HashMap<String, ArrayList<T>> hashTable;
    private final int numHashes;
    private final ArrayList<HashFunction<T>> hashFunctions;

    public HashTable(int numHashes, int w, int dimensions) {
        this.numHashes = numHashes;

        hashTable = new HashMap<>();
        // Generate the hash functions of the hash table
        hashFunctions =  new ArrayList<>();
        for (int i = 0; i < numHashes; i++) {
            hashFunctions.add(new HashFunction<T>(dimensions, w));
        }
    }

    public void add(T entry) {
        String combinedHash = generateCombinedHash(entry);

        if (hashTable.containsKey(combinedHash)) {
            hashTable.get(combinedHash).add(entry);
        } else {
            ArrayList<T> newBucket = new ArrayList<>();
            newBucket.add(entry);
            hashTable.put(combinedHash, newBucket);
        }
    }

    public void remove(T entry) {
        String combinedHash = generateCombinedHash(entry);
        hashTable.get(combinedHash).remove(entry);
    }

    public ArrayList<T> query(T entry) {
        String combinedHash = generateCombinedHash(entry);
        return hashTable.get(combinedHash);
    }

    private String generateCombinedHash(T entry) {
        int[] individualHashes = new int[numHashes];

        for (int f = 0; f < numHashes; f++) {
            individualHashes[f] = hashFunctions.get(f).hash(entry);
        }

        // Combine the individual hashes into one
        return Arrays.toString(individualHashes);
    }
}
