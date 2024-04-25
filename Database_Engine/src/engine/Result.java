package engine;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.Vector;

public class Result implements Iterator {

    private Vector<Hashtable<String, Object>> Tuples;
    private int tupleindex;

    public Result(){
        Tuples = new Vector<>();
    }
    @Override
    public boolean hasNext() {
        return tupleindex < Tuples.size();
    }

    @Override
    public Object next() {
        System.out.println("Entered next");
        if(hasNext())
            return Tuples.get(tupleindex++);
        return null;
    }

    public Vector<Hashtable<String, Object>> getTuples() {
        return Tuples;
    }

    public int getMaxIdx() {
        return tupleindex;
    }

    public String toString(){
        return Tuples.toString();
    }
}
