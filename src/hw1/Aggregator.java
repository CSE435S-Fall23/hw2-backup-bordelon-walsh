package hw1;

import java.util.ArrayList;

public class Aggregator {
    private AggregateOperator operator;
    private boolean groupBy;
    private TupleDesc td;
    private ArrayList<Tuple> results;

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
