package engine;

import java.io.*;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Vector;

public class Table implements Serializable {
    private String name;
    //given a page id returns min and max value of it 
    private Hashtable<Integer, Pair> htblPageIdMinMax;
    private String clusteringKey;
    private String ckType;

    private int numOfCols;
    //names of cols in table 
    private Vector<String> columnNames;
    
    //index name retuns column
    private Hashtable<String, String> htblIndexNameColumn;


    
    public static final String TABLE_DIRECTORY = "./src/content/";

    //max pageid so far 
    private int maxIDsoFar;

    public Table(String strTableName, String strClusteringKeyColumn ) {
        this.name = strTableName;
        this.clusteringKey = strClusteringKeyColumn;
        htblPageIdMinMax = new Hashtable<>();
        htblIndexNameColumn = new Hashtable<>();
        maxIDsoFar = -1;
       
    }

    public Hashtable<String, String> getHtblIndexNameColumn() {
        return htblIndexNameColumn;
    }

    public int getNumOfCols() {
        return numOfCols;
    }

    public void setNumOfCols(int numOfCols) {
        this.numOfCols = numOfCols;
    }

    public String getCkType() {
        return ckType;
    }

    public void setCkType(String ckType) {
        this.ckType = ckType;
    }

    public void serialize() throws IOException {
        ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(TABLE_DIRECTORY + this.getName() + ".ser"));
        outputStream.writeObject(this);
        outputStream.close();
    
    }


    public static Table deserialize(String tableName) throws IOException, ClassNotFoundException {
        ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(TABLE_DIRECTORY + tableName + ".ser"));
        Table table = (Table) inputStream.readObject();

        inputStream.close();
        return table;
    }



    public void setClusteringKey(String clusteringKey) {
        this.clusteringKey = clusteringKey;
    }

    public Integer getPageIDToInsert(Comparable value){
    	// performs a binary search on the sorted list of page IDs to find the appropriate page ID for insertion.
    	//it retrieves the minimum and maximum values associated with the current page ID, without having to load it into memory. 
    	//If the given value falls within the range defined by the minimum and maximum values of the current page, it returns the current page ID.
    	//else it adjusts the search 
    	
        Vector<Integer> sortedID = new Vector<>(this.htblPageIdMinMax.keySet());
        Collections.sort(sortedID);

        int left = 0;
        int right = sortedID.size() - 1;

        while (left <= right) {
            int mid = (right + left) / 2;
            Pair pair = this.getHtblPageIdMinMax().get(sortedID.get(mid));
            Object min = pair.getMin();
            Object max = pair.getMax();

            if (value.compareTo(min) > 0 && value.compareTo(max) < 0)
                return sortedID.get(mid);
            if (value.compareTo(min) < 0)
                right = mid - 1;
            else
                left = mid + 1;
        }
        return sortedID.get(Math.max(right, 0));
    }

    public Page getPageToInsert(Comparable ckValue) throws IOException, ClassNotFoundException {

        //no available pages
        if(this.getHtblPageIdMinMax().isEmpty())
            return null;

        Integer id = this.getPageIDToInsert(ckValue);
        return Page.deserialize(this.getName(), id);
    }


    public int getNextID(Page page) {
        Vector<Integer> idsInTable = new Vector<>(this.htblPageIdMinMax.keySet());
        Collections.sort(idsInTable);

        int index = idsInTable.indexOf(page.getId());

        if (index == idsInTable.size() - 1)
            return -1;
        
        return idsInTable.get(1 + index);
    }

    public int getMaxIDsoFar() {
        return maxIDsoFar;
    }

    public void setMaxIDsoFar(int maxIDsoFar) {
        this.maxIDsoFar = maxIDsoFar;
    }

    public int binarySearchInTable(Comparable value) throws Exception {
    	//this method efficiently searches for a value within a sorted hashtable of ranges. 
//    	 the value falls within any of the defined ranges, it returns the corresponding key; otherwise, it returns -1.
    	//returns page id value should belong to 
        Vector<Integer> sortedID = new Vector<Integer>(this.htblPageIdMinMax.keySet());
        // vector of all page ids 
        Collections.sort(sortedID);

        int left = 0;
        int right = sortedID.size() - 1;

        while (left <= right) {
            int mid = (right + left) / 2;
            Pair pair = this.getHtblPageIdMinMax().get(sortedID.get(mid));
            Object min = pair.getMin();
            Object max = pair.getMax();

            if (value.compareTo(min) >= 0 && value.compareTo(max) <= 0)
                return sortedID.get(mid);
            if (value.compareTo(min) < 0)
                right = mid - 1;
            else
                left = mid + 1;
        }
        return -1;
    }

    
    
    
    
    
    //get the page containing the tuple to be modified based on the given clustering key value. 
    //If the tuple exists, it returns the corresponding page; otherwise, it returns null
    public Page getPageToModify(Comparable ckValue) throws Exception {
        int locatedPageID = this.binarySearchInTable(((Comparable) ckValue));

        if (locatedPageID == -1)
            return null;
        return Page.deserialize(this.getName(), locatedPageID);
    }


    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Hashtable<Integer, Pair> getHtblPageIdMinMax() {
        return htblPageIdMinMax;
    }

  

    public void setHtblPageIdMinMax(Hashtable<Integer, Pair> htblPageIdMaxMin) {
        this.htblPageIdMinMax = htblPageIdMaxMin;
    }

    public String getClusteringKey() {
        return clusteringKey;
    }

    public void setCKName(String clusteringKey) {
        this.clusteringKey = clusteringKey;
    }

    public Vector<String> getColumnNames() {
        return columnNames;
    }

    public void setColumnNames(Vector<String> columnNames) {
        this.columnNames = columnNames;
    }


    public boolean hasPage(int id) {
        return this.getHtblPageIdMinMax().get(id) != null;
    }

    
    

    public void updatePageDelete(Page locatedPage) throws Exception {
        if (locatedPage.isEmpty()) {
            // delete .ser file of page if it becomes empty 
            String pagePath = locatedPage.getPath();
            File file = new File(pagePath);
            if(file.exists()) {
            file.delete();
            }

            this.getHtblPageIdMinMax().remove(locatedPage.getId());
            
            System.out.println("Page deleted due to underflow");
        } else {
            this.setMinMax(locatedPage);
            locatedPage.serialize();
        }
    }

    public void setMinMax(Page page) {
    	//sets a new min and max value for ck for a given page 
        String ck = this.getClusteringKey();
        Pair newPair = new Pair(page.getTuples().get(0).get(ck), page.getTuples().get(page.getTuples().size() - 1).get(ck));
        this.getHtblPageIdMinMax().put(page.getId(), newPair);
    }

    public boolean isEmpty(){
        return this.htblPageIdMinMax.isEmpty();
    }


}
