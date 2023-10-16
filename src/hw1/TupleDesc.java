package hw1;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc {

	private Type[] types;
	private String[] fields;
	
    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr array specifying the number of and types of fields in
     *        this TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	//your code here
    	//make use of already present variables
    	this.types = typeAr;
    	this.fields = fieldAr;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        //your code here
    	//return the length of fields
    	return fields.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        //your code here
    	//return the field at the index
    	return fields[i];
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int nameToId(String name) throws NoSuchElementException {
        //your code here
    	//check each item for the name
    	for(int i = 0; i < fields.length; i++) {
    		if(fields[i].equals(name)) {
    			return i;
    		}
    	}
    	//if it cannot be found, throw an exception
    	throw new NoSuchElementException("Element does not exist");
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getType(int i) throws NoSuchElementException {
        //your code here
    	return types[i];
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	//your code here
    	int sum = 0;
    	//check to see which type we are dealing with and add the needed bytes
    	for(int i = 0; i < types.length; i++) {
    		if(getType(i) == Type.INT) {
    			sum+=4;
    		}
    		if(getType(i) == Type.STRING) {
    			sum+=129;
    		}
    	}
    	return sum;
    }

    /**
     * Compares the specified object with this TupleDesc for equality.
     * Two TupleDescs are considered equal if they are the same size and if the
     * n-th type in this TupleDesc is equal to the n-th type in td.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
    	//your code here
    	
    	//if the sizes are equal and the length of their types are the same
    	if(this.getSize() == ((TupleDesc) o).getSize() && this.types.length == ((TupleDesc) o).types.length) {
    		//we should check to see that each type is the same
    		for(int i = 0; i < types.length; i++) {
    			if(this.types[i] != ((TupleDesc) o).types[i]) {
    				//if there is an issue, return false
    				return false;
    			}
    		}
    		//no issues, return true
    		return true;
    	}
    	return false;
    }
    

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
    	
    	//equality for this class was defined above as being the size and types
    	//multiply the size by the hashcodes of the types
    	//doesn't need to be used
    	
    	int hash = getSize();
    	for(Type t : types) {
    		hash *= t.hashCode();
    	}
    	return hash;
    	
        //throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * @return String describing this descriptor.
     */
    public String toString() {
        //your code here
    	String description = "";
    	for(int i = 0; i < types.length; i++) {
    		description += types[i].toString() + " ";
    		description += "(" + fields[i]+ "), ";
    	}
    	return description;
    }
}