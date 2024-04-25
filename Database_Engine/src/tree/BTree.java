package tree;
import java.io.Serializable;
import java.util.Vector;




//import tree.BTreeLeafNode;


/**
 * A B+ tree
 * Since the structures and behaviors between internal node and external node are different, 
 * so there are two different classes for each kind of node.
 * @param <TKey> the data type of the key
 * @param <TValue> the data type of the value
 */
public class BTree<TKey extends Comparable<TKey>, TValue> implements Serializable {
	private BTreeNode<TKey> root;
	private Vector<TKey> keys;
	
	public BTree() {
		this.root = new BTreeLeafNode<TKey, TValue>();
		keys=new Vector<>();
	}

	/**
	 * Insert a new key and its associated value into the B+ tree.
	 */
	public void insert(TKey key, TValue value) {
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		leaf.insertKey(key, value);
		if (!keys.contains(key)) {
	        keys.add(key);
	    }
		if (leaf.isOverflow()) {
			BTreeNode<TKey> n = leaf.dealOverflow();
			if (n != null)
				this.root = n; 
		}
	}
	
	public boolean update(TKey key, TValue oldValue, TValue newValue) {
	    BTreeLeafNode<TKey, TValue> leaf = findLeafNodeShouldContainKey(key);
	    if (leaf != null) {
	        return leaf.update(key, oldValue, newValue);
	    }
	    return false;
	}

	
	/**
	 * Search a key value on the tree and return its associated value.
	 */
	public Vector<TValue> search(TKey key) {
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		
		int index = leaf.search(key);
		return (index == -1) ? null : leaf.getValue(index);
	}
	
	/**
	 * Delete a key and its associated value from the tree.
	 */
	public void delete(TKey key, TValue value) {
	    BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
	    
	    // Delete the key-value pair from the leaf node
	    if (leaf.delete(key, value) && leaf.isUnderflow()) {
	    	
	        // Handle underflow if necessary
	        BTreeNode<TKey> n = leaf.dealUnderflow();
	        if (n != null) {
	            this.root = n;
	        }
	    }
	    if(this.search(key)==null) {
	    	 if (keys.contains(key)) {
	    	        keys.remove(key);
	    	    }
	    }
	}
	
	public void updaterow(TKey oldKey, TKey newKey, TValue value) {
        this.delete(oldKey, value);
        this.insert(newKey, value);
        System.out.println("Inside BTree, updating row complete in index");
    }

	
	/**
	 * Search the leaf node which should contain the specified key
	 */
	@SuppressWarnings("unchecked")
	private BTreeLeafNode<TKey, TValue> findLeafNodeShouldContainKey(TKey key) {
		BTreeNode<TKey> node = this.root;
		while (node.getNodeType() == TreeNodeType.InnerNode) {
			node = ((BTreeInnerNode<TKey>)node).getChild( node.search(key) );
		}
		
		return (BTreeLeafNode<TKey, TValue>)node;
	}
	
	
	
	public Vector<TValue> searchGreaterThanOrEqual(TKey key) {
	    Vector<TValue> accumulator = new Vector<>();
	    for (TKey currentKey : keys) {
	        if (currentKey.compareTo(key) >= 0) {
	            // If the current key is greater than or equal to the given key,
	            // call the existing search method to get the values associated with it
	            Vector<TValue> values = this.search(currentKey);
	            if (values != null) {
	                // If values are found, add them to the accumulator
	                accumulator.addAll(values);
	            }
	        }
	    }
	    return accumulator;
	}
	public Vector<TValue> searchGreaterThan(TKey key) {
	    Vector<TValue> accumulator = new Vector<>();
	    for (TKey currentKey : keys) {
	        if (currentKey.compareTo(key) > 0) {
	            // If the current key is greater than the given key,
	            // call the existing search method to get the values associated with it
	            Vector<TValue> values = this.search(currentKey);
	            if (values != null) {
	                // If values are found, add them to the accumulator
	                accumulator.addAll(values);
	            }
	        }
	    }
	    return accumulator;
	}
	
	public Vector<TValue> searchLessThan(TKey key) {
	    Vector<TValue> accumulator = new Vector<>();
	    for (TKey currentKey : keys) {
	        if (currentKey.compareTo(key) < 0) {
	            // If the current key is less than the given key,
	            // call the existing search method to get the values associated with it
	            Vector<TValue> values = this.search(currentKey);
	            if (values != null) {
	                // If values are found, add them to the accumulator
	                accumulator.addAll(values);
	            }
	        }
	    }
	    return accumulator;
	}
	
	public Vector<TValue> searchLessThanOrEqual(TKey key) {
	    Vector<TValue> accumulator = new Vector<>();
	    for (TKey currentKey : keys) {
	        if (currentKey.compareTo(key) <= 0) {
	            // If the current key is less than or equal to the given key,
	            // call the existing search method to get the values associated with it
	            Vector<TValue> values = this.search(currentKey);
	            if (values != null) {
	                // If values are found, add them to the accumulator
	                accumulator.addAll(values);
	            }
	        }
	    }
	    return accumulator;
	}
	
	public Vector<TValue> searchNotEqual(TKey key) {
	    Vector<TValue> accumulator = new Vector<>();
	    for (TKey currentKey : keys) {
	        if (!currentKey.equals(key)) {
	            // If the current key is not equal to the given key,
	            // call the existing search method to get the values associated with it
	            Vector<TValue> values = this.search(currentKey);
	            if (values != null) {
	                // If values are found, add them to the accumulator
	                accumulator.addAll(values);
	            }
	        }
	    }
	    return accumulator;
	}


	
	



	
	
	public static void main (String [] args) {
		BTree<Integer,Integer> t1= new BTree<>();
		
		

		t1.insert(1,1);
		t1.insert(1,2);
		t1.insert(1, 3);
		
		t1.insert(3, 1);
		t1.insert(3, 9);
		t1.insert(5, 5);
		t1.insert(5, 5);
		t1.insert(5, 50);
		t1.insert(7, 7);
		t1.insert(8, 8);
		t1.insert(6, 6);
		
	//try 10. 
	System.out.println(t1.search(3));
		t1.update(3, 2,3);	
		System.out.println(t1.search(3));
		
		//System.out.println(t1.searchNotEqual(1));
		
		
//		System.out.println(t1.search(2));
//		System.out.println(t1.search(3));
//		System.out.println(t1.search(5));
//		System.out.println(t1.search(6));
//		System.out.println(t1.search(7));
//		System.out.println(t1.search(8));
		

		
		


		
	}
}
