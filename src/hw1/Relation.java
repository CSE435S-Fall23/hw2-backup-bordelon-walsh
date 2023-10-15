package hw1;

import java.util.ArrayList;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		//your code here
		//make use of what what we have
		this.td = td;
		this.tuples = l;
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		//your code here
		ArrayList<Tuple> selected = new ArrayList<Tuple>();
		
		//for each tuple if the comparison is true, add it to the selected tuples
		for(Tuple t : tuples) {
			if(t.getField(field).compare(op, operand)) {
				selected.add(t);
			}
		}
		
		return new Relation(selected, td);
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
		//your code here
		
		Type[] T = new Type[td.numFields()];
		String[] S = new String[td.numFields()];
		
		//get a copy of the types and names from td
		for(int i = 0; i < td.numFields(); i++) {
			T[i] = td.getType(i);
			S[i] = td.getFieldName(i);
		}
		
		//rename needed fields
		for(int i = 0; i < fields.size(); i++) {
			S[fields.get(i)] = names.get(i);
		}
		
		//new desc created
		TupleDesc TupleDesc = new TupleDesc(T, S);
		
		ArrayList<Tuple> newTups = new ArrayList<Tuple>();
		
		//get a copy of each tuple and set its desc to the new one
		for(Tuple t : tuples) {
			Tuple remade = t;
			remade.setDesc(TupleDesc);
			newTups.add(remade);
		}
		
		//finally return a new relation with the renamed tuples and desc
		return new Relation(newTups, TupleDesc);
	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
		//your code here
		
		//store needed types and names
		Type[] T = new Type[fields.size()];
		String[] S = new String[fields.size()];
		
		//get them from the current tuple desc
		for(int i = 0; i < fields.size(); i++) {
			T[i] = td.getType(fields.get(i));
			S[i] = td.getFieldName(fields.get(i));
		}
		
		TupleDesc newTD = new TupleDesc(T, S);
		
		ArrayList<Tuple> newTups = new ArrayList<Tuple>();
		
		//now get every tuple and give them the new TD
		for(Tuple t : tuples) {
			Tuple remade = t;
			remade.setDesc(newTD);
			newTups.add(remade);
		}
		
		return new Relation(newTups, newTD);
	}
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
	    // Get the TupleDesc of the current relation
	    TupleDesc td1 = this.td;

	    // Get the TupleDesc of the other relation
	    TupleDesc td2 = other.td;

	    // Create a new TupleDesc for the resulting relation by combining the fields from both relations
	    Type[] T = new Type[td1.numFields() + td2.numFields()];
	    String[] S = new String[td1.numFields() + td2.numFields()];

	    // Copy the fields and types from the current relation's TupleDesc
	    for (int i = 0; i < td1.numFields(); i++) {
	        T[i] = td1.getType(i);
	        S[i] = td1.getFieldName(i);
	    }

	    // Copy the fields and types from the other relation's TupleDesc
	    for (int i = 0; i < td2.numFields(); i++) {
	        T[td1.numFields() + i] = td2.getType(i);
	        S[td1.numFields() + i] = td2.getFieldName(i);
	    }

	    // Create a new TupleDesc for the resulting relation
	    TupleDesc resultTd = new TupleDesc(T, S);

	    // Create a list to store the resulting tuples
	    ArrayList<Tuple> resultTuples = new ArrayList<>();

	    // Perform the join operation
	    for (Tuple t1 : this.tuples) {
	        for (Tuple t2 : other.tuples) {
	            // Check if the values in the specified fields are equal
	            if (t1.getField(field1).equals(t2.getField(field2))) {
	                // Create a new tuple for the resulting relation by combining the fields from both tuples
	                Tuple resultTuple = new Tuple(resultTd);

	                // Copy the fields from the current tuple (t1)
	                for (int i = 0; i < td1.numFields(); i++) {
	                    resultTuple.setField(i, t1.getField(i));
	                }

	                // Copy the fields from the other tuple (t2)
	                for (int i = 0; i < td2.numFields(); i++) {
	                    resultTuple.setField(td1.numFields() + i, t2.getField(i));
	                }

	                // Add the resulting tuple to the list
	                resultTuples.add(resultTuple);
	            }
	        }
	    }

	    // Create and return a new relation with the resulting tuples and TupleDesc
	    return new Relation(resultTuples, resultTd);
	}

	
	/**
	 * Performs an aggregation operation on a relation.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return a new Relation with the aggregation results
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {

	    // Create a new TupleDesc for the resulting relation
	    TupleDesc resultTd;

	    if (groupBy) {
	        // For aggregates with GROUP BY, the resulting relation will have two columns:
	        // - The first column will be the group column (same as the current relation)
	        // - The second column will contain the result of aggregation

	        resultTd = new TupleDesc(new Type[] { td.getType(0), Type.INT }, new String[] { td.getFieldName(0), "AggregateResult" });
	    } else {
	        // For aggregates without GROUP BY, the resulting relation will have a single column
	        // containing the result of aggregation

	        resultTd = new TupleDesc(new Type[] { Type.INT }, new String[] { "AggregateResult" });
	    }

	    // Create a list to store the resulting tuples
	    ArrayList<Tuple> resultTuples = new ArrayList<Tuple>();

	    // If there are no tuples, return an empty relation with the result TupleDesc
	    if (this.tuples.isEmpty()) {
	        return new Relation(resultTuples, resultTd);
	    }

	    // Use the Aggregator class to perform aggregation
	    Aggregator aggregator = new Aggregator(op, groupBy, resultTd);

	    /*
	    if (groupBy) {
	        // Aggregates with GROUP BY
	        aggregator = new Aggregator(op, groupBy, resultTd);
	    } else {
	        // Aggregates without GROUP BY
	        aggregator = new Aggregator(op, groupBy, resultTd);
	    }
	    */

	    // Merge each tuple into the aggregator
	    for (Tuple t : this.tuples) {
	        aggregator.merge(t);
	    }

	    // Get the result from the aggregator
	    resultTuples = aggregator.getResults();

	    // Create and return a new relation with the resulting tuples and TupleDesc
	    return new Relation(resultTuples, resultTd);
	}



	
	public TupleDesc getDesc() {
	    return this.td;
	}

	
	public ArrayList<Tuple> getTuples() {
	    return this.tuples;
	}

	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		//your code here
		return null;
	}
}
