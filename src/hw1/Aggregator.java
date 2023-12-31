/*
 * Authors: Robert Walsh, Jayce Bordelon
 */

package hw1;

import java.util.ArrayList;

public class Aggregator {
    private AggregateOperator operator;
    private boolean groupBy;
    private TupleDesc td;
    private ArrayList<Tuple> results;
    private int[] avg;
    private ArrayList<int[]> averages;

    public Aggregator(AggregateOperator operator, boolean groupBy, TupleDesc td) {
        this.operator = operator;
        this.groupBy = groupBy;
        this.td = td;
        this.results = new ArrayList<Tuple>();
        this.averages = new ArrayList<int[]>();
    }

    /**
     * Merges the given tuple into the current aggregation.
     * @param t the tuple to be aggregated
     */
    public void merge(Tuple t) {
        IntField field = (IntField) t.getField(0);

        if (groupBy) {
        	field = (IntField) t.getField(1);
            Field groupField = t.getField(0);
            Tuple existingTuple = findTupleWithGroupField(groupField);

            if (existingTuple != null) {
                IntField resultField = (IntField) existingTuple.getField(1); // Assuming the result field is at index 1

                // Perform aggregation based on the operator
                switch (operator) {
                    case AVG:
                    	//find which average and get new value
                    	for(int i = 0; i < results.size(); i++) {
                    		if(results.get(i).getField(0).equals(existingTuple.getField(0))) {
                    			averages.get(i)[0] += 1;
                    			averages.get(i)[1] += resultField.getValue();
                    			int newAvg = averages.get(i)[1]/averages.get(i)[0];
                    			existingTuple.setField(1, new IntField(newAvg));
                    		}
                    	}
                        //int newCount = ((IntField) existingTuple.getField(2)).getValue() + 1; // Assuming count is at index 2
                        //int newTotal = resultField.getValue() + field.getValue();
                        //existingTuple.setField(2, new IntField(newCount)); // Update count
                        //existingTuple.setField(1, new IntField(newTotal));  // Update total value
                        break;

                    case COUNT:
                        int newCountValue = ((IntField) existingTuple.getField(1)).getValue() + 1; // Assuming count is at index 2
                        existingTuple.setField(1, new IntField(newCountValue));
                        break;

                    case MAX:
                        int currentMaxValue = resultField.getValue();
                        if (field.getValue() > currentMaxValue) {
                            existingTuple.setField(1, new IntField(field.getValue())); // Assuming the result field is at index 1
                        }
                        break;

                    case MIN:
                        int currentMinValue = resultField.getValue();
                        if (field.getValue() < currentMinValue) {
                            existingTuple.setField(1, new IntField(field.getValue())); // Assuming the result field is at index 1
                        }
                        break;

                    case SUM:
                        int currentSumValue = resultField.getValue() + field.getValue();
                        existingTuple.setField(1, new IntField(currentSumValue)); // Assuming the result field is at index 1
                        break;
                }
                
            } else {
                // If the groupField is not in the results, create a new tuple and add it to results
                Tuple newTuple = new Tuple(td);
                newTuple.setField(0, groupField);
                field = (IntField) t.getField(1);
                switch (operator) {
                    case AVG:
                    	int[] tupAvg = new int[] {1, field.getValue()};
                    	averages.add(tupAvg);
                        // Initialize count and total value for AVG
                        //newTuple.setField(1, new IntField(1)); // Assuming count is at index 2
                        newTuple.setField(1, field); // Assuming the result field is at index 1
                        break;
                    case COUNT:
                        // Initialize count for COUNT
                        newTuple.setField(1, new IntField(1)); // Assuming count is at index 2
                        break;
                    case MAX:
                        // Initialize MAX
                        newTuple.setField(1, field); // Assuming the result field is at index 1
                        break;
                    case MIN:
                        // Initialize MIN
                        newTuple.setField(1, field); // Assuming the result field is at index 1
                        break;
                    case SUM:
                        // Initialize SUM
                        newTuple.setField(1, field); // Assuming the result field is at index 1
                        break;
                }
                results.add(newTuple);
            }
            
        } else {
            // Handle non-grouped aggregation
            if (results.isEmpty()) {
                //results.add(t);
                switch(operator) {
                	case AVG:
                		avg = new int[] {1, field.getValue()};
                		results.add(t);
                		break;
                	case COUNT:
                		results.add(t);
                		IntField init = new IntField(1);
                		results.get(0).setField(0, init);
                		break;
                	default:
                		results.add(t);
                		break;
                }
            } else {
                IntField currentResult = (IntField) results.get(0).getField(0);
                int currentValue = currentResult.getValue();
                int newValue;

                switch (operator) {
                    case AVG:
                        // Calculate average
                    	avg[0] +=1;
                    	avg[1] += field.getValue();
                    	newValue = avg[1]/avg[0];
                    	/*
                        int currentCount = ((IntField) results.get(0).getField(1)).getValue(); // Assuming count is at index 1
                        int newCount = currentCount + 1;
                        results.get(0).setField(1, new IntField(newCount)); // Update count
                        newValue = (currentValue * currentCount + field.getValue()) / newCount;
                        */
                        break;
                    case COUNT:
                        // Count the number of tuples
                        newValue = currentValue + 1;
                        break;
                    case MAX:
                        // Calculate maximum
                        if (field.getValue() > currentValue) {
                            newValue = field.getValue();
                        } else {
                            newValue = currentValue;
                        }
                        break;
                    case MIN:
                        // Calculate minimum
                        if (field.getValue() < currentValue) {
                            newValue = field.getValue();
                        } else {
                            newValue = currentValue;
                        }
                        break;
                    case SUM:
                        // Calculate sum
                        newValue = currentValue + field.getValue();
                        break;
                    default:
                        newValue = currentValue;
                        break;
                }

                results.get(0).setField(0, new IntField(newValue));
            }
        }
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
