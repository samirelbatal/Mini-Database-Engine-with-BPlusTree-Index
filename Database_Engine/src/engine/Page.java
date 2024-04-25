package engine;

import java.io.*;
import java.util.*;
public class Page implements Serializable {
	//each row consists of hashtable<key,value>: key represents col name, valure represents corresponding value.
    private Vector<Hashtable<String,Object>> tuples;
    private int id;
    private int maxPageSize;
    private String tableName;

    private static String PAGE_DIRECTORY = "./src/content/";
    private String path;

    public Page(String tableName, int id) throws IOException{
        this.tableName =  tableName;
        tuples = new Vector<>();
        this.id = id;
        this.path = PAGE_DIRECTORY + tableName + "-" + id + ".ser";

        maxPageSize = Integer.parseInt(readConfig(DBApp.CONFIG_LOC).getProperty("MaximumRowsCountinPage"));
    }
    public int getMaxPageSize() {
        return maxPageSize;
    }


    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setTuples(Vector<Hashtable<String,Object>> tuples) {
        this.tuples = tuples;
    }

    public Vector<Hashtable<String,Object>> getTuples() {
        return tuples;
    }

    public void serialize() throws IOException {
        ObjectOutputStream outputStream = new ObjectOutputStream(new FileOutputStream(path));
        outputStream.writeObject(this);
        outputStream.close();
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    // Method to deserialize the engine.Page object
    public static Page deserialize(String strTableName, Integer id) throws IOException, ClassNotFoundException {
        ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(PAGE_DIRECTORY + strTableName + "-" + id + ".ser"));
        Page page = (Page) inputStream.readObject();
        inputStream.close();
        return page;
    }

    public static Properties readConfig(String path) throws IOException {
        Properties properties = new Properties();
        FileInputStream inputStream = new FileInputStream(path);
        properties.load(inputStream);
        inputStream.close();
        return properties;
    }

    //This method is useful for quickly locating a tuple within a page based on its clustering key value. 
    //If no match is found, it returns -1 to indicate that the ckValue was not found in any tuple within the page.
    public int binarySearchInPage(String ckName, Comparable ckValue) {
        Vector<Hashtable<String, Object>> tuples = this.getTuples();

        int left = 0;
        int right = tuples.size() - 1;

        while (left <= right) {
            int mid = (left + right) / 2;

            if (((Comparable) tuples.get(mid).get(ckName)).compareTo(ckValue) == 0)
                return mid;

            if (((Comparable) tuples.get(mid).get(ckName)).compareTo(ckValue) > 0)
                right = mid - 1;
            else
                left = mid + 1;
        }

        return -1;
    }

    public boolean isFull(){
        if(this.tuples.size() == maxPageSize)
            return true;
        return false;
    }

    public boolean isOverFlow(){
        return (this.tuples.size() > maxPageSize);
    }


    public boolean isEmpty(){
        return this.tuples.size() == 0;
    }
     
    
   



    

}
