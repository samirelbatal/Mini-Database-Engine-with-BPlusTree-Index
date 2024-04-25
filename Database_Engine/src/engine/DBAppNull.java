package engine;

import java.io.Serializable;

public class DBAppNull implements Serializable {
    private static DBAppNull instance = null;


    private DBAppNull(){

    }

    // Static method to get the singleton instance of the class
    public static DBAppNull getInstance() {
        if(instance == null)
            instance = new DBAppNull();

        return instance;

    }

    @Override
    public String toString() {
        return "null";
    }

}
