package engine;

import java.io.*;

import java.sql.ResultSet;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import tree.*;

import java.text.ParseException;

public class DBApp {

	private Vector<String> tableNames;
	private Vector<String> dataTypes;
	private int maxNoRowsInPage;
	private int maxEntriesInNode = 20;

	public static final String METADATA_LOC = "./src/content/metadata.csv";
	public static final String TEMP_LOC = "./src/content/temp.csv";
	public static final String CONFIG_LOC = "./src/content/DBApp.config";

	public DBApp() {
		init();

	}

//-------------------------------------------------------------------------------------------------------- initialize atts and read config file
	public void init() {
		// INITIALIZE VALUES ACCORDING TO CONFIG FILE
		dataTypes = new Vector<>();
		Collections.addAll(dataTypes, "java.lang.Integer", "java.lang.Double", "java.lang.String");

		try {
			tableNames = getTableNames();
		} catch (Exception e) {

		}

		try {
			Properties properties = readConfig(CONFIG_LOC);
			maxNoRowsInPage = Integer.parseInt(properties.getProperty("MaximumRowsCountinPage"));
		} catch (Exception e) {

		}
	}

	public static Properties readConfig(String path) throws IOException {
		Properties properties = new Properties();
		FileInputStream inputStream = new FileInputStream(path);
		properties.load(inputStream);
		inputStream.close();
		return properties;
	}

	public Vector<String> getTableNames() throws IOException {

		Vector<String> tableNames = new Vector<>();

		String line = null;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(METADATA_LOC));
			line = br.readLine();
		} catch (Exception ignored) {
			System.out.println("Can't find metadata");
			return new Vector<>();
		}

		while (line != null) {
			String[] content = line.split(",");
			String tableName = content[0];
			String colName = content[1];
			String colType = content[2];

			if (!tableNames.contains(tableName))
				tableNames.add(tableName);

			line = br.readLine();
		}

		br.close();
		return tableNames;
	}

//--------------------------------------------------------------------------------------------------------------------------------------- Create table & index 
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {

		if (tableNames.contains(strTableName))
			throw new DBAppException("Table already exists");

		for (Map.Entry<String, String> entry : htblColNameType.entrySet()) {
			String value = entry.getValue();
			String colName = entry.getKey();

			if (!dataTypes.contains(value))
				throw new DBAppException("Invalid data type");

		}

		if (htblColNameType.get(strClusteringKeyColumn) == null)
			throw new DBAppException("Invalid Primary Key");

		try {

			Table table = new Table(strTableName, strClusteringKeyColumn);
			table.setCkType(htblColNameType.get(strClusteringKeyColumn));
			table.setNumOfCols(htblColNameType.size());
			table.setColumnNames(new Vector<>(htblColNameType.keySet()));

			tableNames.add(strTableName);

			table.serialize();

			writeInCSV(strTableName, strClusteringKeyColumn, htblColNameType); // a method that will write in the csv
		} catch (Exception e) {
			e.printStackTrace();
			throw new DBAppException(e.getMessage());

		}

	}
	
	

	public void createIndex(String strTableName, String strColName, String strIndexName) throws Exception {

		checkIndex(strTableName, strColName);

		String colNameWithIndex = changeCsvForIndex(strTableName, strColName, strIndexName);
		String indexType = "b+tree";
		String type="";

		Table table = Table.deserialize(strTableName);

		// Insert into the index hashtable. key = index name, value= column index is on
		table.getHtblIndexNameColumn().put(strIndexName, strColName);
		
		
		BufferedReader br = new BufferedReader(new FileReader(METADATA_LOC));
		String line = br.readLine();
		while (line != null) {
			String[] content = line.split(",");
			String tableName = content[0];
			String colName = content[1];
			String colType = content[2];

			if (tableName.equals(strTableName) && colName.equals(strColName)) {
				type=colType;
				break;
				
			}
			line = br.readLine();
		}
		br.close();
		
		 if ("java.lang.Integer".equals(type)) {
			 BTree<Integer,Integer> bplustree = new BTree<>();
			 Index index = new Index(strTableName, strIndexName, strColName, bplustree);
			 index.populate();
			 System.out.println("The B+Tree is of type Int with Int refrences for index "+strIndexName);
			 index.serialize();
			 
		    } else if ("java.lang.Double".equals(type)) {
		    	BTree<Double,Integer> bplustree = new BTree<>();
				Index index = new Index(strTableName, strIndexName, strColName, bplustree);
				index.populate();
				System.out.println("The B+Tree is of type Double with Int refrences "+strIndexName);
				index.serialize();
		    } else if ("java.lang.String".equals(type)) {
		    	BTree<String,Integer> bplustree = new BTree<>();
				Index index = new Index(strTableName, strIndexName, strColName, bplustree);
				index.populate();
				System.out.println("The B+Tree is of type String with Int refrences "+strIndexName);
				index.serialize();
		    } else {
		        throw new IllegalArgumentException("Unsupported column data type: " + type);
		    }

		
		table.serialize();
	}
	

	public void checkIndex(String strTableName, String strColName) throws Exception {
		if (!tableNames.contains(strTableName))
			throw new DBAppException("Table not found");

		Table table = Table.deserialize(strTableName);
		BufferedReader br = new BufferedReader(new FileReader(METADATA_LOC));
		String line = br.readLine();
		while (line != null) {
			String[] content = line.split(",");
			String tableName = content[0];
			String colName = content[1];
			String indexName = content[4];

			if (tableName.equals(strTableName) && colName.equals(strColName) && !indexName.equals("null")) {
				br.close();
				throw new DBAppException("Table already has an index on this column");
			}
			line = br.readLine();
		}
		br.close();
		table.serialize();
	}

	public String changeCsvForIndex(String strTableName, String strColName, String indexname) throws Exception {
		BufferedReader reader = new BufferedReader(new FileReader(METADATA_LOC));
		// USE A TEMP FILE TO STORE DATA WHILE PERFORMING CHECKS AND UPDATES
		File tempFile = new File(TEMP_LOC);
		PrintWriter writer = new PrintWriter(tempFile);
		String modifiedColName = null;

		String line = reader.readLine();
		while (line != null) {
			String[] content = line.split(",");
			String table = content[0];
			String colName = content[1];

			if (!table.equals(strTableName) || !colName.equals(strColName)) {
				writer.println(line);
			} else {
				// I found the line I want to change using the column name input
				String colType = content[2];
				String isClusteringKey = content[3];
				// String indexName = strColName + "Index";
				String indexType = "b+tree";

				String modifiedRow = table + "," + colName + "," + colType + "," + isClusteringKey + "," + indexname
						+ "," + indexType;
				writer.println(modifiedRow);
				modifiedColName = colName;
			}
			line = reader.readLine();
		}
		reader.close();
		writer.close();
		File originalFile = new File(METADATA_LOC);
		originalFile.delete();
		tempFile.renameTo(originalFile);
		return modifiedColName;
	}

	public void writeInCSV(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws FileNotFoundException {
		PrintWriter pw = new PrintWriter(new FileOutputStream(METADATA_LOC, true));
		String isClusteringKey;
		for (String key : htblColNameType.keySet()) {
			if (key.equals(strClusteringKeyColumn))
				isClusteringKey = "True";
			else
				isClusteringKey = "False";
			String indexName = "null";
			String indexType = "null";
			String row = strTableName + "," + key + "," + htblColNameType.get(key) + "," + isClusteringKey + ","
					+ indexName + "," + indexType;
			pw.append(row + "\r\n");
		}
		pw.close();
	}

//---------------------------------------------------------------------------------------------------------------------------------------------------------------------- INSERT 

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {

		try {
			checkInsert(strTableName, htblColNameValue);
			// New row with all values lower case if string
			htblColNameValue = changeStringInHashtable(htblColNameValue);

			Table table = Table.deserialize(strTableName);
			// Type cast the pk of the row i wanna insert into a comparable
			Comparable ckValue = (Comparable) htblColNameValue.get(table.getClusteringKey());

			// Insert according to pk, find correct page it belongs to as we insert in order 
			Page pageFound = table.getPageToInsert(ckValue);

			// ensure that all columns in the table have corresponding values during the
			// insertion process.
			// If any values are missing, it populates those columns with the DBAppNull
			if (htblColNameValue.size() < table.getNumOfCols()) {
				for (String colName : table.getColumnNames())
					if (htblColNameValue.get(colName) == null)
						htblColNameValue.put(colName, DBAppNull.getInstance());
			}

			// ? (table.getMaxIDsoFar() + 1): This is the value that will be assigned to pid
			// if the condition (locatedPage == null)
			// locatedPage.getId(): This is the value that will be assigned to pid if the
			// condition (locatedPage == null) is false
			// determine the reference (ID) of the page where the insertion will occur.
			// pid is the page number that i will insert into

			int pid = (pageFound == null) ? (table.getMaxIDsoFar() + 1) : pageFound.getId();

			if (!table.isEmpty() && ckValue.compareTo(table.getHtblPageIdMinMax().get(pid).getMax()) >= 0
					&& pageFound.isFull())
				pid++;
			// Ensures that the table is not empty.
			// Compares the clustering key value (ckValue) with the maximum value of the
			// clustering key of the page with ID pid.
			// If the clustering key value is greater than or equal to the maximum value,
			// it suggests that the insertion should happen in the next page (pid++). or
			// does this if the page is full as well

			// insert in index
			for (String indexName : table.getHtblIndexNameColumn().keySet()) {
				Index index = Index.deserialize(table.getName(), indexName);
				index.insert(htblColNameValue, pid);
				// ------
				index.serialize();

			}
			// It inserts the values into the index at the specified page pid. inserts for
			// all indicies in the table, pass by entire row
			// and each index updates it accordingly to the coloumn its based on.
			// basically(checks if the current page is full and if the insertion needs to
			// happen in the next page based on the clustering key value. Then, it iterates
			// over each index associated with the table, inserts the values into the
			// indexes at the specified page, and serializes the indexes.)

			// insert in page
			if (pageFound == null)
				pageFound = createOverflowPage(htblColNameValue, table);
			    
			
			else
				insertWithShift(htblColNameValue, pageFound.getId(), table);

			table.serialize();
			// If locatedPage is null, it means that there was no suitable page found for
			// the insertion, so it calls the newPageInit method to initialize a new page
			// and assigns the returned page to locatedPage and inserts the tuple init
			// If locatedPage is not null, it means that a suitable page for insertion was
			// found, so it calls the insertAndShift method to insert the tuple into the
			// located page and shift accordingly.

		} catch (Exception e) {
			e.printStackTrace();
			throw new DBAppException(e.getMessage());
		}
	}

	// Helper
	public Hashtable<String, Object> changeStringInHashtable(Hashtable<String, Object> htblColNameValue) {
		// It replaces the original value in the Hashtable with the lowercase version
		for (String column : htblColNameValue.keySet()) {
			if (htblColNameValue.get(column) instanceof String) {
				String lower = (String) ((String) htblColNameValue.get(column)).toLowerCase();
				htblColNameValue.put(column, lower);
			}
		}
		return htblColNameValue;
	}

	//changes here updates refernce. added parameter in method. 
	// HELPER
	public Page createOverflowPage(Hashtable<String, Object> tuple, Table table) throws IOException, ClassNotFoundException {
		// initializes a new page for the table, inserts a tuple into it, updates the
		// table's metadata related to page IDs sets maxpagid to maxpageid+1
		// and clustering key values, and then serializes the new page
		table.setMaxIDsoFar(table.getMaxIDsoFar() + 1);
		Page newPage = new Page(table.getName(), table.getMaxIDsoFar());
		newPage.getTuples().add(tuple);
		int id = newPage.getId();

		Object ckValue = tuple.get(table.getClusteringKey());

		table.getHtblPageIdMinMax().put(id, new Pair(ckValue, ckValue));

		newPage.serialize();
		System.out.println("New Page Created & serialized");
		return newPage;
	}

	// HELPER
	public void insertWithShift(Hashtable<String, Object> tuple, int currentpid, Table table)
			throws IOException, ClassNotFoundException, DBAppException {
		// inserting a tuple into a specific page of a table and handling any necessary
		// shifting if the page becomes full after insertion
		if (!table.hasPage(currentpid)) {
			createOverflowPage(tuple, table);
			return;
		}

		// deserialize the page i'm inserting to
		Page foundPage = Page.deserialize(table.getName(), currentpid);
		String ckName = table.getClusteringKey();
		Object ckValue = tuple.get(ckName);

		// inserts using binary insert method so it can be inserted in the page in OLogN

		binaryInsertinPage(tuple, foundPage, ckName);

		// --------------------------------------------------------------- error occurs
		// In updating refernce after updating page in b+tree
		// Check after i inserted in the page if overflow occured
		if (foundPage.isOverFlow()) {
			// overflow occured i update refrence of overflowed tuple to the next page
			Hashtable<String, Object> lastTuple = foundPage.getTuples().remove(foundPage.getTuples().size() - 1);
			// get the next pageid of the one i was supposed to insert into

			int newpageId = table.getNextID(foundPage);
			// update new min and max ck value of a given page
			table.setMinMax(foundPage);
			foundPage.serialize();

			// update refrence of overflowed tuple ib b+tree
			updateIndexPointers(lastTuple, currentpid, newpageId, table);
			// insert it in the new page id(aka next page)
			insertWithShift(lastTuple, newpageId, table);
			System.out.println("Overflowed tuple inserted");
		} else {
			table.setMinMax(foundPage);
			foundPage.serialize();
		}

	}

	// HELPER
	public void updateIndexPointers(Hashtable<String, Object> tuple, int oldID, int newID, Table table)
			throws IOException, ClassNotFoundException {
		// update refrence in b+tree so value points to right page after inserting
		// handles each coloumn within the index class i pass the whole row and it
		// updates it accordingly to the right coloumn
		if (newID == -1)
			newID = table.getMaxIDsoFar() + 1;

		// Loop around all indicies and update the refrence of the tuple aka page id
		for (String indexName : table.getHtblIndexNameColumn().keySet()) {
			Index index = Index.deserialize(table.getName(), indexName);
			index.updateReference(tuple, oldID, newID);
			index.serialize();
		}
	}

	// HELPER
	public static void binaryInsertinPage(Hashtable<String, Object> tuple, Page page, String ck) {
		// inserts the new tuple into the list of tuples while maintaining the order
		// based on the clustering key using binary search
		// inserting in the given page and i compare based on the given ck value
		Vector<Hashtable<String, Object>> tuples = page.getTuples();

		int left = 0;
		int right = tuples.size() - 1;

		while (left <= right) {
			int mid = (left + right) / 2;

			if (((Comparable) tuples.get(mid).get(ck)).compareTo(tuple.get(ck)) > 0)
				right = mid - 1;
			else
				left = mid + 1;
		}

		tuples.add(left, tuple);
	}

	// Helper method to perform checks if i can insert tuple in the table or not
	public void checkInsert(String strTableName, Hashtable<String, Object> htblColNameValue) throws Exception {

		if (!tableNames.contains(strTableName))
			throw new DBAppException("Table not found");

		Table table = Table.deserialize(strTableName);

		if (htblColNameValue.size() > table.getNumOfCols())
			throw new DBAppException("Invalid number of columns entered");

		for (String column : htblColNameValue.keySet())
			if (!table.getColumnNames().contains(column))
				throw new DBAppException(column + " field does not exist in the table");

		String clustringKeyName = table.getClusteringKey();
		Comparable clusteringKeyValue = (Comparable) htblColNameValue.get(clustringKeyName);
		// get the value in the coloumn clustring key of the tuple im trying to insert
		// and perfoms checks on it

		// INTEGRITY CONSTRAINTS
		if (clusteringKeyValue == null) {
			
			throw new DBAppException("Cannot allow null values for Clustering Key");
			
		}

		BufferedReader br = new BufferedReader(new FileReader(METADATA_LOC));

		String line = br.readLine();
		String[] content = line.split(",");

		while (line != null) {

			content = line.split(",");
			// hagat el table men el metadat
			String tableName = content[0];
			String colName = content[1];
			String colType = content[2];

			Object value = htblColNameValue.get(colName);

			if (!tableName.equals(table.getName())) {
				line = br.readLine();
				continue;
			}

			if (value != null) {
				if (!checksimilarType(value, colType)) {
					// System.out.println(colName);
					throw new DBAppException("Incompatible data types");
				}
			}

			line = br.readLine();

		}

		// VERIFY DUP CK
		int pid = table.binarySearchInTable(clusteringKeyValue);
		if (pid != -1) {
			Page p = Page.deserialize(strTableName, pid);
			int tupleIdx = p.binarySearchInPage(table.getClusteringKey(), clusteringKeyValue);
			p.serialize();

			if (tupleIdx != -1)
				throw new DBAppException("Cannot allow duplicate values for Clustering Key");
		}

		br.close();
		table.serialize();
	}

	public static boolean checksimilarType(Object data, String dataType) throws ClassNotFoundException {
		if (data == null)
			return true;
		if (data instanceof DBAppNull)
			return true;
		return data.getClass().equals(Class.forName(dataType));
	}

	public static int compare(Object object, String value) throws ParseException {

		Comparable parsed;

		if (object instanceof Integer) {
			parsed = Integer.parseInt(value);
		} else if (object instanceof Double) {
			parsed = Double.parseDouble(value);
		} else {
			parsed = value;
		}
		// System.out.println(object+" "+parsed+" ");
		return ((Comparable) object).compareTo(parsed);
	}

//----------------------------------------------------------------------------------------------------------------------------------------------- UPDATE 
	// following method updates one row only
	// htblColNameValue holds the key and new value 
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	//i search for the row that has the clustring key given and when i find it i update the given hastable with the exisiting one. 
	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {

		try {
			// check i can perform an update
			checkUpdate(strTableName, strClusteringKeyValue, htblColNameValue);

			// change values in row i want to insert to lower case
			htblColNameValue = changeStringInHashtable(htblColNameValue);

			Table table = Table.deserialize(strTableName);

			// Convert the clustringkey value to the appropriate column key type of table, then type
			// Cast it, into a comparable.
			Comparable clustringValue = (Comparable) parse(strClusteringKeyValue, table.getCkType());

			if (clustringValue instanceof String)
				// Because inside my page, i have all Strings stored as lower case, so when
				// searching i convert it to lower case to locate it.
				clustringValue = ((String) clustringValue).toLowerCase();

			// retrieve page tuple is in using o(log(n)) as we compare max and min values of
			// every page using CK of row we want to update.
			Page foundPage = table.getPageToModify(clustringValue);

			// retrieve page id of the tuple we need to modify
			int foundPageID = table.binarySearchInTable(((Comparable) clustringValue));

			// we find the index/ row where the tuple is located in that page using binary
			// search, gets the value in hashtable corresponding to CKname and compares it
			// with value we passing.
			int tuplerow = foundPage.binarySearchInPage(table.getClusteringKey(), ((Comparable) clustringValue));
			if (tuplerow == -1)
				throw new DBAppException("Tuple does not exist");

			// retrieve old tuple that has the given clustering key of the method: finds Row
			// to be updated
			Hashtable<String, Object> tupleToUpdate = foundPage.getTuples().get(tuplerow);

			for (String idxName : table.getHtblIndexNameColumn().keySet()) {
				Index index = Index.deserialize(table.getName(), idxName);
				// update indicies delete value of old tuples and replace with new values with
				// ref of same pageid
				if (!index.getColumnName().equals(table.getClusteringKey())) {
					index.updateIndex(tupleToUpdate, htblColNameValue, foundPageID);
					index.serialize();
				}
			}

			// update content of the row in the actual page .
			tupleToUpdate.putAll(htblColNameValue);
			// update min and max value in the page
			table.setMinMax(foundPage);
			foundPage.serialize();
			table.serialize();
		} catch (Exception e) {
			e.printStackTrace();
			throw new DBAppException(e.getMessage());
		}

	}

	// Helper method to ensure i can update the row 
	public void checkUpdate(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue) throws Exception {
		// Check if table name parameter exists
		if (!tableNames.contains(strTableName))
			throw new DBAppException("engine.Table not found");

		Table table = Table.deserialize(strTableName);

		// 	Check if the coloumn clustring key is not in the hashtable can't correspond to a value
		if (htblColNameValue.get(table.getClusteringKey()) != null) {
			throw new DBAppException("Cannot update the clustering key");
		}

		// Loop on all columns name in the ht given as parameter and check if they exist
		for (String column : htblColNameValue.keySet()) {
			if (!table.getColumnNames().contains(column)) {
				throw new DBAppException(column + " field does not exist in the table");
			}
		}

		// Read from meta data file
		BufferedReader br = new BufferedReader(new FileReader(METADATA_LOC));

		String line = br.readLine();
		String[] content = line.split(",");

		while (line != null) {
			content = line.split(",");
			String tableName = content[0];
			String colName = content[1];
			String colType = content[2];
			boolean isClusteringKey = Boolean.parseBoolean(content[3]);

			Object value = htblColNameValue.get(colName);

			if (!tableName.equals(table.getName())) {
				line = br.readLine();
				continue;
			}

			// Check if type given as parameter is same as the type associated with colname
			// in meta date file
			if (value != null) {
				if (!checksimilarType(value, colType)) {
					throw new DBAppException("Incompatible data types");
				}

			}
			if (isClusteringKey) {
				try {
					parse(strClusteringKeyValue, colType);
				} catch (Exception e) {
					throw new DBAppException("Invalid data type for clustering key");
				}
			}

			line = br.readLine();
		}
		table.serialize();
		br.close();
	}

//convert the strClusteringKeyValue into the appropriate data type specified by table.getCkType()
	public Object parse(String value, String type) throws ClassNotFoundException, ParseException {
		return switch (type) {
		case "java.lang.Integer" -> Integer.parseInt(value);
		case "java.lang.Double" -> Double.parseDouble(value);

		default -> value;
		};

	}

//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------DELETE 
	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search 
	// to identify which rows/tuples to delete. 	
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {

		// Binary search for page according to pk value
		// once I found the page, binary search for row
		// once I found row comapre with row i wanna delete if they are equal then delete
		// from indicies and page
		// check if underflow occured for the page, if so delete the page .

		try {
			checkDelete(strTableName, htblColNameValue);
			htblColNameValue = changeStringInHashtable(htblColNameValue);
			Table table = Table.deserialize(strTableName);
			Hashtable<String, String> htblIdxNameCol=table.getHtblIndexNameColumn();

			// Store clusteringkey index name
			String CKIndex = hasIndexOnCk(table);

			// check if enetered row has a clustering key
			boolean hasCKValue = htblColNameValue.get(table.getClusteringKey()) != null;
			// store clustering key value of row we deleting
			

			if (hasCKValue) {

				// DELETE ONE ROW
				System.out.println("DELETING 1 ROW ACCORDING TO ck Given");
				Comparable clustringValue = (Comparable) htblColNameValue.get(table.getClusteringKey());

				Page foundPage = null;

					// No index on clustering key: Binary search for page it belongs to
					System.out.println("Searching for single row using binary search");
					// binary search to locate the page of row according to the clustring key
					foundPage = table.getPageToModify(clustringValue);
					
					if(foundPage ==null) {
						throw new DBAppException("Row doesn't exist");

					}

				// Binary search to locate row inside page

				int tupleIndex = foundPage.binarySearchInPage(table.getClusteringKey(), ((Comparable) clustringValue));

				if (tupleIndex == -1) {
					throw new DBAppException("Row doesn't exist");
				}

				Hashtable<String, Object> tuple_to_delete = foundPage.getTuples().get(tupleIndex);

				if (!isEqual(htblColNameValue, tuple_to_delete))
					throw new DBAppException("Row doesn't exist");

				// REMOVE IT FROM ALL INDEX
				int pageid=foundPage.getId();

				for (String IndexName : table.getHtblIndexNameColumn().keySet()) {
					Index index = Index.deserialize(strTableName, IndexName);
					index.deleteIndex(tuple_to_delete,pageid);
					index.serialize();
				}

				// delete tuple from page
				foundPage.getTuples().remove(tuple_to_delete);
				System.out.println("Row deleted from page");
				// update table pages after deletion. Deletes page file if underflow occurs or
				// page becomes empty, and set new min max of table 

				table.updatePageDelete(foundPage);

			}

			
			else {
				// no CK DELETE ALL ROWS ACCORDING TO VALUES
				// note values in hashtable are anded, example if given name-omar, gpa=0.8.
				// delete all values that have name =omar and gpa=0.8
				Vector<Integer> searchpageids = null;
				//search through table if there is an index use it
				String indexname=findIndexForColumn(htblIdxNameCol,htblColNameValue);
				 if (!indexname.equals("")) {
					 System.out.println("Searching for pages to delete from using index "+indexname);
					 Index index = Index.deserialize(strTableName, indexname);
	                    searchpageids = index.searchPagedeleteIndex(htblColNameValue);
	                    index.serialize();
				 }
				 else {
					 System.out.println("Searching for pages to delete from using linear search");
					 searchpageids = new Vector<Integer>(table.getHtblPageIdMinMax().keySet());
				 }
				 System.out.println("Deleting from pages: "+searchpageids);
				 //stores pageid and a vector of all its tuples
				 Vector<Integer> visitedpages =  new Vector<>();				 
				 for (Integer id : searchpageids) {
					    if(!visitedpages.contains(id)) {
						Vector<Hashtable<String, Object>> tmp = new Vector<>();

	                    Page currPage = Page.deserialize(table.getName(), id);
	                    for (Hashtable<String, Object> row : currPage.getTuples())
	                        if (isEqual(htblColNameValue, row)) {
	                            tmp.add(row);
	                            for (String IndexName : table.getHtblIndexNameColumn().keySet()) {
	            					Index index = Index.deserialize(strTableName, IndexName);
	            					index.deleteIndex(row,currPage.getId());
	            					index.serialize();
	            				}
	                            
	                        }
	                    

//	                    holds the page and the rows so i can delete from index
//	                    htblIdRows.put(id,tmp);

	                    currPage.getTuples().removeAll(tmp);
	                    table.updatePageDelete(currPage);
	                    visitedpages.add(id);
					    }
	                    
	                }
				 
			}
			table.serialize();

		}

		catch (Exception e) {
			e.printStackTrace();
			throw new DBAppException(e.getMessage());
		}

	}
	public String findIndexForColumn(Hashtable<String, String> htblIdxNameCol, Hashtable<String, Object> htblColNameValue) {
	    String indexName = "";

	    // Iterate through each column name in the provided hashtable
	    for (String colName : htblColNameValue.keySet()) {
	        // Check if the provided hashtable contains the column name
	        if (htblIdxNameCol.containsValue(colName)) {
	            // Iterate through the entries of the hashtable to find the corresponding index name
	            for (Map.Entry<String, String> entry : htblIdxNameCol.entrySet()) {
	                if (entry.getValue().equals(colName)) {
	                    // Found the index name corresponding to the column name
	                    indexName = entry.getKey();
	                    break;
	                }
	            }
	            // Assuming only one indexed column is used, so we break after finding the first match
	            break;
	        }
	    }
	    
	    return indexName;
	}


	public void checkDelete(String strTableName, Hashtable<String, Object> htblColNameValue) throws Exception {

		if (!tableNames.contains(strTableName))
			throw new DBAppException("Table not found");

		Table table = Table.deserialize(strTableName);

		BufferedReader br = new BufferedReader(new FileReader(METADATA_LOC));

		String line = br.readLine();
		String[] content = line.split(",");

		while (line != null) {
			content = line.split(",");
			String tableName = content[0];
			String colName = content[1];
			String colType = content[2];
			Object value = htblColNameValue.get(colName);

			if (!tableName.equals(table.getName())) {
				line = br.readLine();
				continue;
			}

			if (value != null) {
				if (!checksimilarType(value, colType)) {
					// System.out.println(colName);
					throw new DBAppException("Incompatible data types");
				}

			}

			line = br.readLine();
		}

		br.close();

		for (String column : htblColNameValue.keySet())
			if (!table.getColumnNames().contains(column))
				throw new DBAppException(column + " field does not exist in the table");

		table.serialize();
	}

	//compares to hashtables ensures they are equal in values. 
	public boolean isEqual(Hashtable<String, Object> htblColNameValue, Hashtable<String, Object> tuple) {
		for (String colName : htblColNameValue.keySet()) {
			if (!htblColNameValue.get(colName).equals(tuple.get(colName))) // not all conditions satisfied
				return false;
		}
		return true;
	}

	public String hasIndexOnCk(Table table) {

		Hashtable<String, String> htblIdxNameCol = table.getHtblIndexNameColumn();

		Vector<String> columns = new Vector<String>();

		for (String idxName : htblIdxNameCol.keySet()) {
			columns.add(htblIdxNameCol.get(idxName));

			if (columns.contains(table.getClusteringKey())) {
				return idxName;
			}
		}
		return null;

	}

//------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws Exception {
		
	    checkSelect(arrSQLTerms, strarrOperators);

	    String tableName = arrSQLTerms[0].get_strTableName();
	    Table table = Table.deserialize(tableName);
	    Hashtable<String, String> htblIdxNameCol = table.getHtblIndexNameColumn();

	    Result r = new Result();

	    String indexFound = "";
	    SQLTerm termFound = null ;

	    // Check if any of the terms columns  have an index, first one i fimnd that the one i will use to filter the pages. 
	    // find the first indexed column among the SQL terms and stop searching once it finds one, assuming that only one indexed column is used.
	    for (SQLTerm sqlTerm : arrSQLTerms) {
	        String colName = sqlTerm.get_strColumnName();
	        if (htblIdxNameCol.containsValue(colName)) {
	        	  for (Map.Entry<String, String> entry : htblIdxNameCol.entrySet()) {
	                  if (entry.getValue().equals(colName)) {
	                      // Found the index name corresponding to the column name
	                      indexFound = entry.getKey();
	                      termFound=sqlTerm;
	                      break;
	                  }
	        }
	        	  break ; // Assuming only one indexed column is used
	    }
	    }

	    if ((!indexFound.equals("")) && andOps(strarrOperators)) {
	        // INDEX BASED, index found and all operators are AND: all result rows will be in output pages of index 
	    	//If an index is found, it retrieves the index from the table and calls the searchSelect method on the index object to get the page references.
	    	//It then iterates over the page references, deserializes each page, and retrieves the tuples.
	    	
	        System.out.println("Using Index "+indexFound+ " to locate pages");
	        Index idx = Index.deserialize(tableName, indexFound);
	        //search for pages that satisfy the single sql term corresponding to the column of the index.  
	        Vector<Integer> pageids = idx.searchSelect(termFound);

	        Vector<Integer> idSeen = new Vector<>();
            //within those pages do linear search and see if tuples satisfy all sql terms.
	        if(pageids!=null) {
	        for (int id : pageids) {
	            if (idSeen.contains(id))
	                continue;
	            idSeen.add(id);
	            Page p = Page.deserialize(tableName, id);

	            for (Hashtable<String, Object> tuple : p.getTuples()) {
	                //determine if the tuple satisfies the conditions specified in the SQLTerm objects (arrSQLTerms)
	            	Vector<Boolean> bools = getCheckRowEqTerm(tuple, arrSQLTerms);
	            	// This condition checks if the boolean result obtained from evaluating the conditions (bools) satisfies the logical operators specified in strarrOperators. 
	            	// If the condition is true, meaning all the conditions are met based on the specified operators, the tuple is added to the result set 
	                if (boolres(bools, strarrOperators))
	                    r.getTuples().add(tuple);
	            }
	        }
	    }
	        idx.serialize();
	    } else {
	        // LINEAR SCAN TABLE
	    	// If no index is found, it performs a linear scan of the table, deserializing each page and checking each tuple for satisfaction with the SQL terms and operators.
	    	
	    	System.out.println("Using Linear Search");
	        for (int id : table.getHtblPageIdMinMax().keySet()) {
	            Page page = Page.deserialize(tableName, id);
	            for (Hashtable<String, Object> tuple : page.getTuples()) {
	                Vector<Boolean> bools = getCheckRowEqTerm(tuple, arrSQLTerms);
	                if (boolres(bools, strarrOperators)) {
	                    r.getTuples().add(tuple);
	                }
	            }
	        }
	    }

	    table.serialize();
	    return r;
	}

	public static Vector<String> getKeySetAsVector(Hashtable<String, Hashtable<Object, Vector<Integer>>> hashtable) {
		Vector<String> keySetVector = new Vector<>();
		for (String key : hashtable.keySet()) {
			keySetVector.add(key);
		}
		return keySetVector;
	}

	public void checkSelect(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws Exception {
		// size of [] operator has to be less than size of sub queries by 1
		if (strarrOperators.length != arrSQLTerms.length - 1)
			throw new DBAppException("Number of operators is not valid");
		// check operator is logical
		
		for (String s : strarrOperators) {
			
			if (s.equals("OR") || s.equals("AND") || s.equals("XOR"))
				continue;
			throw new DBAppException("Wrong operator");
		}
		
		// within each sub query check that
		for (SQLTerm term : arrSQLTerms) {
			term.checkArithmaticOperator();
			// check details of col and table and datatype of each term
			checkSelect2(term);
		}
	}

	// ensure elements from the select statemnt exist in meta data : coloumn name & table 
	public void checkSelect2(SQLTerm term) throws IOException, DBAppException, ClassNotFoundException {
		if (!tableNames.contains(term.get_strTableName())) {
			throw new DBAppException("Table does not exist");
		}
		Table table = Table.deserialize(term.get_strTableName());
		if (!table.getColumnNames().contains(term.get_strColumnName())) {
			throw new DBAppException("Column" + term.get_strColumnName() + "does not exist");
		}
		table.serialize();

		BufferedReader br = new BufferedReader(new FileReader(METADATA_LOC));
		String line = br.readLine();

		while (line != null) {
			String[] content = line.split(",");
			String tableName = content[0];
			String colName = content[1];
			String colType = content[2];
			if (!colName.equals(term.get_strColumnName())) {
				line = br.readLine();
				continue;
			}

			// checks if the type of the value stored in the SQLTerm object (term) matches
			// the type specified in the metadata for the column
			if (!sameType1(term.get_objValue(), colType))
				throw new DBAppException("Wrong Type ");

			line = br.readLine();
		}
		br.close();
	}

	//perform comparisons and logical operations between objects check if two objects satisfy the condition based on the given operator.  
	public static boolean compareObjs(Object oper1, Object oper2, String operator) {

		if (!(oper1 instanceof DBAppNull) && !(oper2 == null)) {
			Comparable oper3 = (Comparable) oper1;
			return switch (operator) {
			case "=" -> oper3.equals(oper2);
			case "!=" -> !oper3.equals(oper2);
			case "AND" -> (boolean) oper3 && (boolean) oper2;
			case ">" -> oper3.compareTo(oper2) > 0;
			case "OR" -> (boolean) oper3 || (boolean) oper2;
			case ">=" -> oper3.compareTo(oper2) >= 0;
			case "<" -> oper3.compareTo(oper2) < 0;
			case "<=" -> oper3.compareTo(oper2) <= 0;
			case "XOR" -> ((boolean) oper3 && !((boolean) oper2)) || (!((boolean) oper3) && (boolean) oper2);

			default -> false;
			};
		} else if (((oper1 instanceof DBAppNull) && !(oper2 == null))
				|| (!(oper1 instanceof DBAppNull) && (oper2 == null))) {
			if (operator.equals("!="))
				return true;
			return false;
		} else {
			if (operator.equals("="))
				return true;
			else
				return false;
		}
	}

	// get accumlated boolean value after performing boolean operations such as
	// 'and,or' on a list of boolean values such as "true,false"

	public static boolean boolres(Vector<Boolean> boolArr, String[] strarrOperators) {
		boolean result = boolArr.remove(0);

		for (int i = 0; i < boolArr.size(); i++) {
			boolean op1 = boolArr.get(i);
			String operator = strarrOperators[i];
			result = compareObjs(result, op1, operator);
		}
		return result;

	}
	
	
	
	// These methods facilitate the evaluation of conditions specified by SQL terms against a given tuple, allowing you to determine if the tuple meets the criteria specified by one or more SQL terms.
	  public static boolean checkRowEqTerm(Hashtable<String, Object> tuples, SQLTerm term) {
	        String colName = term.get_strColumnName();
	        return compareObjs(tuples.get(colName), term.get_objValue(), term.get_strOperator());
	    }

	  // Returns the total boolean values, of the row in comparison to each term, for each term returns true or false and combine them in a list. this list is then compared to check if row acc belongs or not.
	    public static Vector<Boolean> getCheckRowEqTerm(Hashtable<String, Object> tuples, SQLTerm[] terms) {
	        Vector<Boolean> res = new Vector<>();
	        for (SQLTerm term : terms)
	            res.add(checkRowEqTerm(tuples, term));
	        return res;
	    }
	    
	    public static boolean andOps(String[] strarrOperators) {
	        for (String operator : strarrOperators)
	            if (!operator.equals("AND"))
	                return false;
	        return true;
	    }
	
	    public static boolean sameType1(Object data, String dataType) {
	        if (data == null || data instanceof DBAppNull) {
	            // If data is null or an instance of DBAppNull, return true
	            return true;
	        }

	        // Map data types to their corresponding classes
	        Map<String, Class<?>> typeMap = new HashMap<>();
	        typeMap.put("java.lang.Integer", Integer.class);
	        typeMap.put("java.lang.Double", Double.class);
	        typeMap.put("java.lang.String", String.class);

	        // Check if the data type exists in the map
	        if (!typeMap.containsKey(dataType)) {
	            throw new IllegalArgumentException("Unsupported data type: " + dataType);
	        }

	        // Get the expected class for the given data type
	        Class<?> expectedClass = typeMap.get(dataType);

	        // Check if the actual class of the data matches the expected class
	        return expectedClass.isInstance(data);
	    }

	    
	
	
	    
	  
	
	    
	    //Method takes SQL term & operators and a hashtable page tuple and returns true or false 
	

//-----------------------------------------------------------------------------------------------------
	public static void main(String[] args) {

		try {
			//String strTableName = "Student";
			DBApp dbApp = new DBApp();
			
			
//			dbApp.tableNames.add("Teacher");


//---------------------------------------------------------------------------------------------- print page content 
//System.out.println("STUDENT:");
//Page p1= (Page) Page.deserialize("Student", 3);
//Page p2= (Page) Page.deserialize("Student", 5);
//Page p3= (Page) Page.deserialize("Student", 6);
//System.out.println(p1.getTuples());
//System.out.println(p2.getTuples());
//System.out.println(p3.getTuples());
//
//System.out.println("TEACHER:");
//Page p4= (Page) Page.deserialize("Teacher", 0);
////Page p5= (Page) Page.deserialize("Teacher", 2);
//System.out.println(p4.getTuples());
////System.out.println(p5.getTuples());



//------------------------------------------------------------------update row 
//Hashtable htblColNameValue = new Hashtable( );

//htblColNameValue.put("sname", new String("zaky noor"));
//htblColNameValue.put("gpa", new Double( 0.3 ) );
//dbApp.updateTable("Student", 
//		"23498",
//		htblColNameValue);

//System.out.println(bt1.search(j.stringToASCII("halwagy"))); 

//System.out.println(bt1.search(j.stringToASCII("omar gaber")));

//------------------------------------------------------------------------- create table and indexs and insertions

			Hashtable htblColNameType = new Hashtable();
//			htblColNameType.put("sid", "java.lang.Integer");
//			htblColNameType.put("sname", "java.lang.String");
//			htblColNameType.put("gpa", "java.lang.Double");
//			dbApp.createTable( "Student", "sid", htblColNameType);
//			dbApp.createIndex( "Student", "sid", "sidIndex");
//			
//			htblColNameType.clear();
//			htblColNameType.put("tid", "java.lang.Integer");
//			htblColNameType.put("tname", "java.lang.String");
//			htblColNameType.put("rank", "java.lang.Double");
//			dbApp.createTable( "Teacher", "tid", htblColNameType);
//			dbApp.createIndex( "Teacher", "tname", "tnameIndex");
//			dbApp.createIndex( "Teacher", "rank", "rankIndex");
//
//			Hashtable htblColNameValue = new Hashtable();
//			htblColNameValue.put("sid", new Integer(2343432));
//			htblColNameValue.put("sname", new String("Ahmed Noor"));
//			htblColNameValue.put("gpa", new Double(0.95));
//			dbApp.insertIntoTable("Student", htblColNameValue);

//			htblColNameValue.clear();
//			htblColNameValue.put("sid", new Integer(453455));
//			htblColNameValue.put("sname", new String("Omar Noor"));
//			htblColNameValue.put("gpa", new Double(0.95));
//			dbApp.insertIntoTable("Student", htblColNameValue);

//			htblColNameValue.clear();
//			htblColNameValue.put("sid", new Integer(5674567));
//			htblColNameValue.put("sname", new String("Dalia Noor"));
//			htblColNameValue.put("gpa", new Double(1.25));
//			dbApp.insertIntoTable("Student", htblColNameValue);

//			htblColNameValue.clear();
//			htblColNameValue.put("sid", new Integer(23498));
//			htblColNameValue.put("sname", new String("Ahmed Noor"));
//			htblColNameValue.put("gpa", new Double(1.5));
//		dbApp.insertIntoTable("Student", htblColNameValue);

//		htblColNameValue.clear();
//			htblColNameValue.put("sid", new Integer(78452));
//			htblColNameValue.put("sname", new String("Zaky Noor"));
//			htblColNameValue.put("gpa", new Double(0.88));
//			dbApp.insertIntoTable("Student", htblColNameValue);

//			htblColNameValue.clear();
//			htblColNameValue.put("tid", new Integer(78452));
//			htblColNameValue.put("tname", new String("ahmed hany"));
//			htblColNameValue.put("rank", new Double(0.5));
//			dbApp.insertIntoTable("Teacher", htblColNameValue);
//			
//			htblColNameValue.clear();
//			htblColNameValue.put("tid", new Integer(12345));
//			htblColNameValue.put("tname", new String("rana amr"));
//			htblColNameValue.put("rank", new Double(1.3));
//			dbApp.insertIntoTable("Teacher", htblColNameValue);
//			htblColNameValue.clear();

//			htblColNameValue.put("tid", new Integer(6789));
//			htblColNameValue.put("tname", new String("ahmed halwagy"));
//			htblColNameValue.put("rank", new Double(2.8));
//		dbApp.insertIntoTable("Teacher", htblColNameValue);



//-------------------------------------------------------------------------------------------------------Delete
//Hashtable htblColNameValue = new Hashtable( );
//htblColNameValue.clear( );
////htblColNameValue.put("id", new Integer( 5674567 ));
////htblColNameValue.put("name", new String("zaky noor" ) );
//htblColNameValue.put("gpa", new Double( 0.95 ) );
//dbApp.deleteFromTable( "Student" , htblColNameValue );

// -------------------------------------------------------------------------------------------------------- 

//SQLTerm[] arrSQLTerms = new SQLTerm[2];
//arrSQLTerms[0] = new SQLTerm("Student","sid","=",new Integer(453455)); 

//arrSQLTerms[0]._strTableName ="Student";
//arrSQLTerms[0]._strColumnName="name";
//arrSQLTerms[0]._strOperator="=";
//arrSQLTerms[0]._objValue="ahmed Noor";

//arrSQLTerms[1] = new SQLTerm("Student","gpa","=",new Double(0.95));
//arrSQLTerms[1]._strTableName ="Student";
//arrSQLTerms[1]._strColumnName="gpa";
//arrSQLTerms[1]._strOperator="=";
//arrSQLTerms[1]._objValue=new Double( 1.5 );


//String[]strarrOperators = new String[1];
//strarrOperators[0] = "AND";


// select * from Student where name = "John Noor" or gpa = 1.5;
//Iterator resultSet = dbApp.selectFromTable(arrSQLTerms , strarrOperators);
//while (resultSet.hasNext()) {
//   Object result = resultSet.next();
//   System.out.println(result);
// }
		} catch (Exception exp) {
			exp.printStackTrace();
		}
		}

	public static void updateIndices(Table table, Hashtable<Integer, Vector<Hashtable<String, Object>>> htblIdTuples)
			throws Exception {
	}
}