package hw1;

import java.util.ArrayList;

public class Aggregator {
    private AggregateOperator operator;
    private boolean groupBy;
    private TupleDesc td;
    private ArrayList<Tuple> results;
    private int[] avg;

    public Aggregator(AggregateOperator operator, boolean groupBy, TupleDesc td) {
        this.operator = operator;
        this.groupBy = groupBy;
        this.td = td;
        this.results = new ArrayList<>();
    }

    /**
     * Merges the given tuple into the current aggregation.
     * @param t the tuple to be aggregated
     */
    public void merge(Tuple t) {
    
    	//get the value of the tuple
    	IntField field = (IntField) t.getField(0);
    	
    	//if we are grouping go through these 5 cases
    	if(groupBy) {
    		
    		if(operator == AggregateOperator.AVG) {
        		
        	}
        	if(operator == AggregateOperator.COUNT) {
        		
        	}
        	if(operator == AggregateOperator.MAX) {
        		
        	}
        	if(operator == AggregateOperator.MIN) {
        		
        	}
        	if(operator == AggregateOperator.SUM) {
        		
        	}
    	}
    	
    	//otherwise do the non-grouped cases
    	else {
    		
    		IntField curr;
    		
    		//keep track of an average array, pos 0 is count pos 1 is total value
    		//adjust the value of result according to new average
    		if(operator == AggregateOperator.AVG) {
        		
    			if(avg == null) {
        			avg = new int[] {1, field.getValue()};
        		}
        		else {
        			curr = (IntField) results.get(0).getField(0);
        			avg[0] += 1;
        			avg[1] += curr.getValue();
        		}
        		IntField average = new IntField(avg[1]/avg[0]);
        		
        		if(results.isEmpty()) {
        			results.add(t);
        		}
        		results.get(0).setField(0, average);
        	}
    		
    		//start with 1 and increment as new tuples are added
        	if(operator == AggregateOperator.COUNT) {
        		
        		if(results.isEmpty()) {
        			IntField init = new IntField(1);
        			t.setField(0, init);
        			results.add(t);
        		}
        		else {
        			IntField curCount = (IntField) results.get(0).getField(0);
        			IntField incremented = new IntField(curCount.getValue()+1);
        			results.get(0).setField(0, incremented);
        		}
        	}
        	
        	//if the current value is lower than new value set the new value to current
        	if(operator == AggregateOperator.MAX) {
        		
        		if(results.isEmpty()) {
        			results.add(t);
        			return;
        		}
        		else {
        			curr = (IntField) results.get(0).getField(0);
        		}
        		if(curr.getValue() < field.getValue()) {
        			results.get(0).setField(0, field);
        		}
        		return;
        	}
        	
        	//if the current value is higher than new value set the new value to current
        	if(operator == AggregateOperator.MIN) {
        		
        		if(results.isEmpty()) {
        			results.add(t);
        			return;
        		}
        		else {
        			curr = (IntField) results.get(0).getField(0);
        		}
        		if(curr.getValue() > field.getValue()) {
        			results.get(0).setField(0, field);
        		}
        		return;
        	}
        	
        	//add the values together
        	if(operator == AggregateOperator.SUM) {
        		
        		if(results.isEmpty()) {
        			results.add(t);
        			return;
        		}
        		else {
        			curr = (IntField) results.get(0).getField(0);
        		}
        		
        		int val = curr.getValue() + field.getValue();
    			IntField newVal = new IntField(val);
    			results.get(0).setField(0, newVal);
    			return;
        	}
    	}
    	
    	/*
        if (groupBy) {
            // If grouping is enabled, extract the group field from the tuple
            Field groupField = t.getField(0);
            
            // Find if the groupField already exists in the results
            Tuple existingTuple = findTupleWithGroupField(groupField);

            if (existingTuple != null) {
                // If a tuple with the groupField exists, update its aggregation result
            	
                
            } else {
                // If the groupField is not in the results, create a new tuple
            	Tuple newTuple = new Tuple(t.getDesc());
                
            }
        } else {
            
        }
        */
    }



    /**
     * Finds and returns the first tuple in results with the given groupField, if it exists.
     */
    private Tuple findTupleWithGroupField(Field groupField) {
        for (Tuple tuple : results) {
            if (tuple.getField(0).equals(groupField)) {
                return tuple;
            }
        }
        return null;
    }

    /**
     * Returns the result of the aggregation.
     * @return a list containing the tuples after aggregation
     */
    public ArrayList<Tuple> getResults() {
        return results;
    }
}
