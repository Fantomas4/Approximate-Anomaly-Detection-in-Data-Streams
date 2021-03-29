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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import core.lsh.families.HashFamily;
import core.lsh.families.HashFunction;

/**
 * An index contains one or more locality sensitive hash tables. These hash
 * tables contain the mapping between a combination of a number of hashes
 * (encoded using an integer) and a list of possible nearest neighbours.
 * 
 * @author Joren Six
 */
class HashTable {

	/**
	 * Contains the mapping between a combination of a number of hashes (encoded
	 * using an integer) and a list of possible nearest neighbours
	 */
	private HashMap<String,List<Entry<?>>> hashTable;
	private HashFunction[] hashFunctions;
	private HashFamily family;
	
	/**
	 * Initialize a new hash table, it needs a hash family and a number of hash
	 * functions that should be used.
	 * 
	 * @param numberOfHashes
	 *            The number of hash functions that should be used.
	 * @param family
	 *            The hash function family knows how to create new hash
	 *            functions, and is used therefore.
	 */
	public HashTable(int numberOfHashes,HashFamily family){
		hashTable = new HashMap<>();
		this.hashFunctions = new HashFunction[numberOfHashes];
		for(int i=0;i<numberOfHashes;i++){
			hashFunctions[i] = family.createHashFunction();
		}
		this.family = family;
	}

	/**
	 * Query the hash table for a vector. It calculates the hash for the vector,
	 * and does a lookup in the hash table. If no candidates are found, an empty
	 * list is returned, otherwise, the list of candidates is returned.
	 * 
	 * @param query
	 *            The query vector.
	 * @return Does a lookup in the table for a query using its hash. If no
	 *         candidates are found, an empty list is returned, otherwise, the
	 *         list of candidates is returned.
	 */
		public List<Entry<?>> query(Entry<?> query) {
		String combinedHash = hash(query);
		if(hashTable.containsKey(combinedHash))
			return hashTable.get(combinedHash);
		else
			return new ArrayList<Entry<?>>();
	}

	/**
	 * Add an entry to the index.
	 * @param entry
	 */
	public void add(Entry<?> entry) {
		String combinedHash = hash(entry);
		if(! hashTable.containsKey(combinedHash)){
			hashTable.put(combinedHash, new ArrayList<Entry<?>>());
		}
		hashTable.get(combinedHash).add(entry);
	}

	/**
	 * Remove an entry from the index.
	 * @param entry
	 */
	public void remove(Entry<?> entry) {
		String combinedHash = hash(entry);
		if(hashTable.containsKey(combinedHash)){
			hashTable.remove(combinedHash, new ArrayList<Entry<?>>());
		}
	}
	
	/**
	 * Calculate the combined hash for a vector.
	 * @param entry The vector to calculate the combined hash for.
	 * @return An integer representing a combined hash.
	 */
	private String hash(Entry<?> entry){
		int hashes[] = new int[hashFunctions.length];
		for(int i = 0 ; i < hashFunctions.length ; i++){
			hashes[i] = hashFunctions[i].hash(entry);
		}
		String combinedHash = family.combine(hashes);
		return combinedHash;
	}

	/**
	 * Return the number of hash functions used in the hash table.
	 * @return The number of hash functions used in the hash table.
	 */
	public int getNumberOfHashes() {
		return hashFunctions.length;
	}
}
