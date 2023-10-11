package hw1;

import java.sql.Types;
import java.util.HashMap;

/**
 * This class represents a tuple that will contain a single row's worth of information
 * from a table. It also includes information about where it is stored
 * @author Sam Madden modified by Doug Shook
 *
 */
public class Tuple {
	
	/**
	 * Creates a new tuple with the given description
	 * @param t the schema for this tuple
	 */
	
	//needed variables
	private TupleDesc tup;
	private int Pid;
	private int ID;
	private HashMap<String, Field> map = new HashMap<>();
	
	public Tuple(TupleDesc t) {
		this.tup = t;
	}
	
	public TupleDesc getDesc() {
		//your code here
		return this.tup;
	}
	
	/**
	 * retrieves the page id where this tuple is stored
	 * @return the page id of this tuple
	 */
	
	public int getPid() {
		//your code here
		return this.Pid;
	}

	public void setPid(int pid) {
		//your code here
		this.Pid = pid;
	}

	/**
	 * retrieves the tuple (slot) id of this tuple
	 * @return the slot where this tuple is stored
	 */
	
	public int getId() {
		//your code here
		return this.ID;
	}

	public void setId(int id) {
		//your code here
		this.ID = id;
	}
	
	public void setDesc(TupleDesc td) {
		//your code here;
		this.tup = td;
	}
	
	/**
	 * Stores the given data at the i-th field
	 * @param i the field number to store the data
	 * @param v the data
	 */
	
	public void setField(int i, Field v) {
		//your code here
		
		//use a hashmap and store the field number to the field
		map.put(tup.getFieldName(i), v);
	}
	
	public Field getField(int i) {
		//your code here
		return map.get(tup.getFieldName(i));
	}
	
	/**
	 * Creates a string representation of this tuple that displays its contents.
	 * You should convert the binary data into a readable format (i.e. display the ints in base-10 and convert
	 * the String columns to readable text).
	 */
	public String toString() {
		//your code here
		String str = "";
		for(HashMap.Entry<String, Field> entry : map.entrySet()) {
			str += entry.getKey() + entry.getValue();
		}
		return str;
	}
}