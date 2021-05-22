package core.lsh;

import core.DataObj;

import java.util.*;

public class HashTable<T extends DataObj<T>> {
    private class HashBucket<T extends DataObj<T>> {
        int k;
        ArrayList<T> safeInliers;
        ArrayList<T> bucketEntries;
        Random randomGenerator;

        public HashBucket(int k) {
            this.k = k;

            safeInliers = new ArrayList<>();
            bucketEntries = new ArrayList<>();
            randomGenerator = new Random();
        }

        public void add(T newEntry) {
            while (bucketEntries.size() > k && safeInliers.size() > 0) {
                // Try to reduce bucket size by removing
                // random safe inliers
                int randomIndex = randomGenerator.nextInt(safeInliers.size());
                T removedSafeInlier = safeInliers.remove(randomIndex);
                bucketEntries.remove(removedSafeInlier);
            }

            if (newEntry.count_after >= k) {
                safeInliers.add(newEntry);
            }
            bucketEntries.add(newEntry);
        }

        public void remove(T newEntry) {
            safeInliers.remove(newEntry);
            bucketEntries.remove(newEntry);
        }

        public ArrayList<T> getEntries() {
            return bucketEntries;
        }
    }

    private final HashMap<String, HashBucket<T>> hashTable;
    private final int numHashes;
    private final int k;
    private final ArrayList<HashFunction<T>> hashFunctions;

    public HashTable(int numHashes, int w, int dimensions, int k) {
        this.numHashes = numHashes;
        this.k = k;

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
            HashBucket<T> newBucket = new HashBucket<>(k);
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

        HashBucket<T> resultHashBucket = hashTable.get(combinedHash);
        if (resultHashBucket == null) {
            return new ArrayList<>();
        } else {
            return resultHashBucket.getEntries();
        }
    }

    private String generateCombinedHash(T entry) {
        int[] individualHashes = new int[numHashes];

        for (int f = 0; f < numHashes; f++) {
            individualHashes[f] = hashFunctions.get(f).hash(entry);
        }

        // Combine the individual hashes into one
        return Arrays.toString(individualHashes);
    }

//    // Returns the total amount of entries stored in the Hash Table.
//    public int getSize() {
//        int sum = 0;
//
//        for (ArrayList<T> bucket : hashTable.values()) {
//            sum += bucket.size();
//        }
//
//        return sum;
//    }
//
    public ArrayList<T> getAllEntries() {
        ArrayList<T> allEntries = new ArrayList<>();

        for (HashBucket<T> bucket : hashTable.values()) {
            allEntries.addAll(bucket.getEntries());
        }

        return allEntries;
    }
}
