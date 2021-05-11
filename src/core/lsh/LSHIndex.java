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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import core.lsh.families.DistanceComparator;
import core.lsh.families.DistanceMeasure;
import core.lsh.families.HashFamily;


/**
 * The index makes it easy to store vectors and lookup queries efficiently. For
 * the moment the index is stored in memory. It holds a number of hash tables,
 * each with a couple of hashes. Together they can be used for efficient lookup
 * of nearest neighbours.
 * 
 * @author Joren Six
 */
public class LSHIndex {

	private HashFamily family;
	private List<HashTable> hashTable; 
	private int evaluated;
	
	/**
	 * Create a new index.
	 * 
	 * @param family
	 *            The family of hash functions to use.
	 * @param numberOfHashes
	 *            The number of hashes that are concatenated in each hash table.
	 *            More concatenated hashes means that less candidates are
	 *            selected for evaluation.
	 * @param numberOfHashTables
	 *            The number of hash tables in use, recall increases with the
	 *            number of hash tables. Memory use also increases. Time needed
	 *            to compute a hash also increases marginally.
	 */
	public LSHIndex(HashFamily family, int numberOfHashes, int numberOfHashTables){
		this.family = family;
		hashTable = new ArrayList<HashTable>();
		for(int i = 0 ; i < numberOfHashTables ; i++ ){
			hashTable.add(new HashTable(numberOfHashes, family));
		}
		evaluated = 0;
	}
	
	/**
	 * Add an entry to the current index. The hashes are calculated with the
	 * current hash family and added in the right place.
	 * 
	 * @param entry The entry to add.
	 *
	 */
	public void add(Entry entry) {
		for (HashTable table : hashTable) {
			table.add(entry);
		}
	}

	/**
	 * Remove an entry from the current index.
	 *
	 * @param entry The entry to remove.
	 *
	 */
	public void remove(Entry entry) {
		for (HashTable table : hashTable) {
			table.remove(entry);
		}
	}
	
	/**
	 * The number of hash tables used in the current index.
	 * @return The number of hash tables used in the current index.
	 */
	public int getNumberOfHashTables(){
		return hashTable.size();
	}
	
	/**
	 * The number of hashes used in each hash table in the current index.
	 * @return The number of hashes used in each hash table in the current index.
	 */
	public int getNumberOfHashes(){
		return hashTable.get(0).getNumberOfHashes();
	}

	/**
	 * Query for the k nearest neighbours in using the current index. The
	 * performance (in computing time and recall/precision) depends mainly on
	 * how the current index is constructed and how the underlying data looks.
	 * 
	 * @param query
	 *            The query vector. The center of the neighbourhood.
	 * @param maxSize
	 *            The maximum number of neighbours to return. Beware, the number
	 *            of neighbours returned lays between zero and the chosen
	 *            maximum.
	 * @return A list of nearest neighbours, the number of neighbours returned
	 *         lays between zero and a chosen maximum.
	 */
	public List<Entry> nnQuery(final Entry query, int maxSize){
		Set<Entry> candidateSet = new HashSet<>();
		for(HashTable table : hashTable){
			List<Entry> v = table.query(query);
			candidateSet.addAll(v);
		}
		List<Entry>candidates = new ArrayList<Entry>(candidateSet);
		evaluated += candidates.size();
		DistanceMeasure measure = family.createDistanceMeasure();
		DistanceComparator dc = new DistanceComparator(query, measure);
		Collections.sort(candidates,dc);
		if(candidates.size() > maxSize){
			candidates = candidates.subList(0, maxSize);
		}
		return candidates;
	}

	/**
	 * Query for all the points in the given range from the given query point using the current index.
	 * The performance (in computing time and recall/precision) depends mainly on
	 * how the current index is constructed and how the underlying data looks.
	 *
	 * @param query The query vector. The center of the neighbourhood.
	 *
	 * @return A list of nearest neighbours, the number of neighbours returned
	 *         lays between zero and a chosen maximum.
	 */
	public List<Entry> rangeQuery(final Entry query){
		Set<Entry> candidateSet = new HashSet<>();
		for(HashTable table : hashTable){
			List<Entry> v = table.query(query);
			candidateSet.addAll(v);
		}
		List<Entry>candidates = new ArrayList<Entry>(candidateSet);
//		evaluated += candidates.size();
//		DistanceMeasure measure = family.createDistanceMeasure();
//		DistanceComparator dc = new DistanceComparator(query, measure);
//		Collections.sort(candidates,dc);

		return candidates;
	}
	
	/**
	 * The number of near neighbour candidates that are evaluated during the queries on this index. 
	 * Can be used to calculate the average evaluations per query.
	 * @return The number of near neighbour candidates that are evaluated during the queries on this index. 
	 */
	public int getTouched(){
		return evaluated;
	}

}


