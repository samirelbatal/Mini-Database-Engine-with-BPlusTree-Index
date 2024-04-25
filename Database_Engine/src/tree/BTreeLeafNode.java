package tree;
import java.io.Serializable;
import java.util.Vector;




class BTreeLeafNode<TKey extends Comparable<TKey>, TValue> extends BTreeNode<TKey> implements Serializable {
	protected final static int LEAFORDER = 200;
    private final Vector<Vector<TValue>> values;
	
	public BTreeLeafNode() {
		this.keys = new Object[LEAFORDER + 1];
		this.values = new Vector<>();
        for (int i = 0; i <= LEAFORDER; i++) {
            this.values.add(new Vector<>());
        }
	}

	@SuppressWarnings("unchecked")
	public Vector<TValue> getValue(int index) {
		return this.values.get(index);
	}

	public void setValue(int index, Vector<TValue> value) {
		this.values.set(index, value);
	}
	
	@Override
	public TreeNodeType getNodeType() {
		return TreeNodeType.LeafNode;
	}
	
	@Override
	public int search(TKey key) {
		for (int i = 0; i < this.getKeyCount(); ++i) {
			 int cmp = this.getKey(i).compareTo(key);
			 if (cmp == 0) {
				 return i;
			 }
			 else if (cmp > 0) {
				 return -1;
			 }
		}
		
		return -1;
	}
	
	
	/* The codes below are used to support insertion operation */
	
	public void insertKey(TKey key, TValue value) {
	    int index = 0;
	    while (index < this.getKeyCount() && this.getKey(index).compareTo(key) < 0)
	        ++index;
	    
	    // Check if the key already exists
	    if (index < this.getKeyCount() && this.getKey(index).compareTo(key) == 0) {
	        // Key already exists, add the value to the existing vector
	        Vector<TValue> existingValues = this.getValue(index);
	        existingValues.add(value);
	    } else {
	        // Key doesn't exist, insert a new key and value
	        this.insertAt(index, key, value);
	    }
	}

	private void insertAt(int index, TKey key, TValue value) {
	    // move space for the new key
	    for (int i = this.getKeyCount() - 1; i >= index; --i) {
	        this.setKey(i + 1, this.getKey(i));
	        this.setValue(i + 1, this.getValue(i));
	    }
	    
	    // insert new key and value
	    this.setKey(index, key);
	    Vector<TValue> newValueVector = new Vector<>();
	    newValueVector.add(value);
	    this.setValue(index, newValueVector);
	    ++this.keyCount;
	}

	public boolean update(TKey key, TValue oldValue, TValue newValue) {
	    int index = search(key);
	    if (index != -1) {
	        Vector<TValue> values = getValue(index);
	        for (int i = 0; i < values.size(); i++) {
	            if (values.get(i).equals(oldValue)) {
	                values.set(i, newValue);
	                return true;
	            }
	        }
	    }
	    return false;
	}

	
	/**
	 * When splits a leaf node, the middle key is kept on new node and be pushed to parent node.
	 */
	@Override
	protected BTreeNode<TKey> split() {
	    int midIndex = this.getKeyCount() / 2;
	    
	    BTreeLeafNode<TKey, TValue> newRNode = new BTreeLeafNode<TKey, TValue>();
	    for (int i = midIndex; i < this.getKeyCount(); ++i) {
	        newRNode.setKey(i - midIndex, this.getKey(i));
	        newRNode.setValue(i - midIndex, this.getValue(i));
	        this.setKey(i, null);
	        this.setValue(i, null);
	    }
	    newRNode.keyCount = this.getKeyCount() - midIndex;
	    this.keyCount = midIndex;
	    
	    // Ensure that the new right node points to the correct sibling
	    newRNode.setRightSibling(this.getRightSibling());
	    newRNode.setLeftSibling(this);
	    if (this.getRightSibling() != null) {
	        this.getRightSibling().setLeftSibling(newRNode);
	    }
	    this.setRightSibling(newRNode);
	    
	    return newRNode;
	}

	
	@Override
	protected BTreeNode<TKey> pushUpKey(TKey key, BTreeNode<TKey> leftChild, BTreeNode<TKey> rightNode) {
		throw new UnsupportedOperationException();
	}
	
	
	
	
	/* The codes below are used to support deletion operation */
	
	public boolean delete(TKey key, TValue value) {
	    int index = this.search(key);
	    if (index == -1)
	        return false;

	    boolean deleted = deleteValueAt(index, value);
	    if (deleted && this.getValue(index).size() == 0) {
	        deleteAt(index);
	    }
	    return deleted;
	}

	private boolean deleteValueAt(int index, TValue value) {
	    Vector<TValue> values = this.getValue(index);
	    boolean deleted = values.remove(value);
	    return deleted;
	}

	private void deleteAt(int index) {
	    int i;
	    for (i = index; i < this.getKeyCount() - 1; ++i) {
	        this.setKey(i, this.getKey(i + 1));
	        this.setValue(i, this.getValue(i + 1));
	    }
	    this.setKey(i, null);
	    this.setValue(i, null);
	    --this.keyCount;
	}

	
	@Override
	protected void processChildrenTransfer(BTreeNode<TKey> borrower, BTreeNode<TKey> lender, int borrowIndex) {
		throw new UnsupportedOperationException();
	}
	
	@Override
	protected BTreeNode<TKey> processChildrenFusion(BTreeNode<TKey> leftChild, BTreeNode<TKey> rightChild) {
		throw new UnsupportedOperationException();
	}
	
	/**
	 * Notice that the key sunk from parent is be abandoned. 
	 */
	@Override
	@SuppressWarnings("unchecked")
	protected void fusionWithSibling(TKey sinkKey, BTreeNode<TKey> rightSibling) {
	    BTreeLeafNode<TKey, TValue> siblingLeaf = (BTreeLeafNode<TKey, TValue>) rightSibling;
	    if(siblingLeaf!=null) {
	    int j = this.getKeyCount();
	    for (int i = 0; i < siblingLeaf.getKeyCount(); ++i) {
	        this.setKey(j + i, siblingLeaf.getKey(i));
	        this.setValue(j + i, siblingLeaf.getValue(i));
	    }
	    this.keyCount += siblingLeaf.getKeyCount();

	    this.setRightSibling(siblingLeaf.getRightSibling());
	    if (siblingLeaf.getRightSibling() != null) {
	        siblingLeaf.getRightSibling().setLeftSibling(this);
	    }
	    }
	}

	
	@Override
	@SuppressWarnings("unchecked")
	protected TKey transferFromSibling(TKey sinkKey, BTreeNode<TKey> sibling, int borrowIndex) {
	    BTreeLeafNode<TKey, TValue> siblingNode = (BTreeLeafNode<TKey, TValue>) sibling;

	    // Get the key and its associated vector from the sibling
	    TKey transferredKey = siblingNode.getKey(borrowIndex);
	    Vector<TValue> transferredValues = siblingNode.getValue(borrowIndex);

	    // Insert the key and its associated vector into the current leaf node
	 // Insert each value from the transferred vector individually
	    for (TValue value : transferredValues) {
	        this.insertKey(transferredKey, value);
	    }


	    // Remove the transferred key and values from the sibling
	    siblingNode.deleteAt(borrowIndex);

	    // Return the key depending on the borrow index
	    return borrowIndex == 0 ? transferredKey : this.getKey(0);
	}


}
