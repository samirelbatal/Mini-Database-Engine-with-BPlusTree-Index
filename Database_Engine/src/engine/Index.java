package engine;

import java.io.*;

import java.util.Hashtable;
import java.util.Vector;

import tree.*;
;
public class Index implements Serializable {
    private String tableName;
    private String indexname;//index name created in create index
    public BTree bPlusTree;
    private String columnName;//column used for the index 
    public static final String TABLE_DIRECTORY = "./src/content/";
    
    public Index(String tableName, String indexname, String column, BTree bPlusTree) {
        this.tableName = tableName;
        this.indexname = indexname;
        this.columnName = column;
        this.bPlusTree = bPlusTree;
    }
    
    //get values of this col from pages and insert into b+tree
    public void populate() throws Exception {
        Table table = Table.deserialize(tableName);
        Vector<Integer> ids = new Vector<>(table.getHtblPageIdMinMax().keySet());
        for (Integer id : ids) {
            Page currPage = Page.deserialize(table.getName(), id);
            //tuples in vector of hashtables, 

            for (Hashtable<String, Object> tuple : currPage.getTuples()) {
            	//Within each iteration of the loop, it retrieves the value from the tuple corresponding to the specified column name (columnName). 
            	//This value serves as the key for the B+ tree index. 
                Object keyValue = tuple.get(columnName);
                // get the value corresponding to the key "coloumn name" ex if col name gpa get the value of ot e.g gpa:3.2 return 3.2

               //Object key;
                if (keyValue instanceof Integer) {
                    int key = (Integer) keyValue;
                    bPlusTree.insert(key, id);
                } else if (keyValue instanceof String) {
                    String key = (String) keyValue;
                    bPlusTree.insert(key, id);
                } else if (keyValue instanceof Double) {
                    double key = (double) keyValue ;
                    bPlusTree.insert(key, id);
                } else {
                    throw new IllegalArgumentException("Unsupported key type: " + keyValue.getClass());
                }

                // Insert the key into the B+ tree, key is found in this page id. each value has a corresponding id which is the page it belongs to
              
            }
            currPage.serialize();
        }
        System.out.println("Index created for Column "+columnName+ "Using B+tree");
    }

    
    
    //Insert into a B+Tree same as above but i pass the page it belongs to aka the id parameter 
    public void insert(Hashtable<String, Object> tuple, int page) {
    	System.out.println("Trying to insert in index " + this.indexname);
    	// Get the value corresponding to the key "col name of this index" of the tuple im trying to insert 
    	Object keyValue = tuple.get(columnName);
    	 if (keyValue instanceof Integer) {
             int key = (Integer) keyValue;
             bPlusTree.insert(key, page);
             System.out.println("In BTree "+key+" is inserted with refrence"+bPlusTree.search(key));
         } else if (keyValue instanceof String) {
             String key = (String) keyValue;
             bPlusTree.insert(key, page);
             System.out.println("In BTree "+key+" is inserted with refrence"+bPlusTree.search(key));
         } else if (keyValue instanceof Double) {
             double key = (double) keyValue ;
             bPlusTree.insert(key, page);
             System.out.println("In BTree "+key+" is inserted with refrence"+bPlusTree.search(key));
         } else {
             throw new IllegalArgumentException("Unsupported key type: " + keyValue.getClass());
         }
    	
        
    }
    
    
    
    // FOR INSERT METHOD 
    // update the refrence page of a specific value incase of shifting 
    public void updateReference(Hashtable<String, Object> tuple, int oldPageId, int newPageId) {
    	//update refrence of a vlaue in b+tree incase of shifting for that specific coloumn  
    	Object keyValue = tuple.get(columnName);
    	 if (keyValue instanceof Integer) {
             int key = (Integer) keyValue;
             bPlusTree.update(key, oldPageId, newPageId);
             System.out.println("value is "+ key);
             System.out.println("page is "+newPageId);
             System.out.println("Index updated for overflowed row, changed refrence from "+oldPageId+"to "+newPageId);
         } else if (keyValue instanceof String) {
             String key = (String) keyValue;
             bPlusTree.update(key, oldPageId, newPageId);
             System.out.println("value is "+ key);
             System.out.println("page is "+newPageId);
             System.out.println("Index updated for overflowed row, changed refrence from "+oldPageId+"to "+newPageId);
         } else if (keyValue instanceof Double) {
             double key = (double) keyValue ;
             bPlusTree.update(key, oldPageId, newPageId);
             System.out.println("value is "+ key);
             System.out.println("page is "+newPageId);
             System.out.println("Index updated for overflowed row, changed refrence from "+oldPageId+"to "+newPageId);
         } else {
             throw new IllegalArgumentException("Unsupported key type: " + keyValue.getClass());
         }
        
    }
    
    
 
    
    
    
    // FOR UPDATE METHOD 
    //In updatying a tuple i replace old value with new value while still having the same refrence 
    public void updateIndex(Hashtable<String, Object> oldTuple, Hashtable<String, Object> newTuple, int pageId) {
        // Extract the key value from the old tuple
        Object oldKeyValue = oldTuple.get(columnName);
        Object newKeyValue = newTuple.get(columnName);
        
        if (oldKeyValue instanceof Integer && newKeyValue instanceof Integer) {
            int oldKey = (int) oldKeyValue;
            int newKey = (int) newKeyValue;
            bPlusTree.updaterow(oldKey, newKey, pageId);
           
            
        } else if (oldKeyValue instanceof String && newKeyValue instanceof String) {
            String oldKey = (String) oldKeyValue;
            String newKey = (String) newKeyValue;
            bPlusTree.updaterow(oldKey, newKey, pageId);
           
        } else if (oldKeyValue instanceof Double && newKeyValue instanceof Double) {
            double oldKey = (double) oldKeyValue;
            double newKey = (double) newKeyValue;
            bPlusTree.updaterow(oldKey, newKey, pageId);
          
        } else {
            throw new IllegalArgumentException("Unsupported key type: " + oldKeyValue.getClass());
        }
    }
    

    
    
    
    //DELETE METHOD. Delete value from b+tree 
    //IPASS BY A ROW AND IT LOOKS FOR THE VALUE ASSOCIATED WITH A SPECIFC COLOUMN AND DELETES IT FROM B+TREE
    public void deleteIndex(Hashtable<String, Object> tuple,int page) {
        Object keyValue = tuple.get(columnName);

        if (keyValue instanceof Integer) {
            int key = (Integer) keyValue;
                bPlusTree.delete(key,page);
                System.out.println("Deleting refrence ("+key+","+page+") from BTree complete");
            
        } else if (keyValue instanceof String) {
            String key = (String) keyValue;
            bPlusTree.delete(key,page);
                System.out.println("Deleting refrence ("+key+","+page+") from BTree complete");
            
        } else if (keyValue instanceof Double) {
            double key = (double) keyValue;
            bPlusTree.delete(key,page);
               System.out.println("Deleting refrence ("+key+","+page+") from BTree complete");
            
        } else {
            throw new IllegalArgumentException("Unsupported key type: " + keyValue.getClass());
        }
        
        
    }

    public Vector<Integer> searchPagedeleteIndex(Hashtable<String, Object> tuple) {
    	Object keyValue = tuple.get(columnName);
        if (keyValue instanceof Integer) {
            int key = (Integer) keyValue;
            return bPlusTree.search(key);
            
        } else if (keyValue instanceof String) {
            String key = (String) keyValue;
            return bPlusTree.search(key);
            
        } else if (keyValue instanceof Double) {
            double key = (double) keyValue;
            return bPlusTree.search(key);
        } else {
            throw new IllegalArgumentException("Unsupported key type: " + keyValue.getClass());
        }
    }
    
    
    
    
    //we loop through each SQLTerm to check if the column matches the columnName of our index. 
    //If it does, we perform a search in the B+ tree for the corresponding value and merge the results. 
    //Finally, we return the vector of page references
    public Vector<Integer> searchSelect(SQLTerm term) throws Exception {
        // Initialize the include array

        
            Object value = term.get_objValue();
            // Determine the key based on the value type
            

            // Get the operator from the SQL term
            String operator = term.get_strOperator();
            // Initialize the result set to store keys that satisfy the condition
            Vector<Integer> result = new Vector<>();

            // Perform the search based on the operator
            switch (operator) {
                case "=":
                	if (value instanceof Integer) {
                        int key = (int) value;
                        result = bPlusTree.search(key);
                    } else if (value instanceof String) {
                        String key = (String) value;
                        result = bPlusTree.search(key);
                    } else if (value instanceof Double) {
                       double key = (double) value;
                       result = bPlusTree.search(key);
                    } else {
                        throw new IllegalArgumentException("Unsupported key type: " + value.getClass());
                    }
                    // For equality, directly search for the key
                    
                    break;
                case ">":
                    // For greater than, search for keys greater than the given key
                	if (value instanceof Integer) {
                        int key = (int) value;
                        result = bPlusTree.searchGreaterThan(key);
                    } else if (value instanceof String) {
                        String key = (String) value;
                        result = bPlusTree.searchGreaterThan(key);
                    } else if (value instanceof Double) {
                       double key = (double) value;
                       result = bPlusTree.searchGreaterThan(key);
                    } else {
                        throw new IllegalArgumentException("Unsupported key type: " + value.getClass());
                    }
                    
                    break;
                case "<":
                	if (value instanceof Integer) {
                        int key = (int) value;
                        result = bPlusTree.searchLessThan(key);
                    } else if (value instanceof String) {
                        String key = (String) value;
                        result = bPlusTree.searchLessThan(key);
                    } else if (value instanceof Double) {
                       double key = (double) value;
                       result = bPlusTree.searchLessThan(key);
                    } else {
                        throw new IllegalArgumentException("Unsupported key type: " + value.getClass());
                    }
                    // For less than, search for keys less than the given key
                    
                    break;
                case ">=":
                	if (value instanceof Integer) {
                        int key = (int) value;
                        result = bPlusTree.searchGreaterThanOrEqual(key);
                    } else if (value instanceof String) {
                        String key = (String) value;
                        result = bPlusTree.searchGreaterThanOrEqual(key);
                    } else if (value instanceof Double) {
                       double key = (double) value;
                       result = bPlusTree.searchGreaterThanOrEqual(key);
                    } else {
                        throw new IllegalArgumentException("Unsupported key type: " + value.getClass());
                    }
                    // For greater than or equal to, search for keys greater than or equal to the given key
                    
                    break;
                case "<=":
                	if (value instanceof Integer) {
                        int key = (int) value;
                        result = bPlusTree.searchLessThanOrEqual(key);
                    } else if (value instanceof String) {
                        String key = (String) value;
                        result = bPlusTree.searchLessThanOrEqual(key);
                    } else if (value instanceof Double) {
                       double key = (double) value;
                       result = bPlusTree.searchLessThanOrEqual(key);
                    } else {
                        throw new IllegalArgumentException("Unsupported key type: " + value.getClass());
                    }
                    // For less than or equal to, search for keys less than or equal to the given key
                    
                    break;
                case "!=":
                	if (value instanceof Integer) {
                        int key = (int) value;
                        result = bPlusTree.searchNotEqual(key);
                    } else if (value instanceof String) {
                        String key = (String) value;
                        result = bPlusTree.searchNotEqual(key);
                    } else if (value instanceof Double) {
                       double key = (double) value;
                       result = bPlusTree.searchNotEqual(key);
                    } else {
                        throw new IllegalArgumentException("Unsupported key type: " + value.getClass());
                    }
                    // For not equal to, search for all keys except the given key
                    
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported operator: " + operator);
            }
            
        System.out.println("Will Search in pages: "+result+" for selecting");
        
        return result;
    }



 
   
    //serialize index name 
    public void serialize() throws IOException {
        ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(TABLE_DIRECTORY + tableName + indexname + ".ser"));
        outputStream.writeObject(this);
        outputStream.close();
    }
    
    public static Index deserialize(String tableName, String indexName) throws IOException, ClassNotFoundException {
        ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(TABLE_DIRECTORY + tableName + indexName + ".ser"));
        Index index = (Index) inputStream.readObject();
        inputStream.close();
        return index;
    }

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public String getIndexname() {
		return indexname;
	}

	public void setIndexname(String indexname) {
		this.indexname = indexname;
	}

	public BTree getbPlusTree() {
		return bPlusTree;
	}

	public void setbPlusTree(BTree bPlusTree) {
		this.bPlusTree = bPlusTree;
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
    
}