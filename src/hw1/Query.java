package hw1;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

public class Query {

    private String q;

    public Query(String q) {
        this.q = q;
    }

    public Relation execute() {
        Statement statement = null;
        try {
            statement = CCJSqlParserUtil.parse(q);
        } catch (JSQLParserException e) {
            System.out.println("Unable to parse query");
            e.printStackTrace();
        }
        Select selectStatement = (Select) statement;
        PlainSelect sb = (PlainSelect) selectStatement.getSelectBody();

        // A catalog to get the tables as needed
        Catalog c = Database.getCatalog();

        // Which columns does this query want?
        List<SelectItem> selected = sb.getSelectItems();

        // First part of building a TupleDesc
        String[] names = new String[selected.size()];
        for (int i = 0; i < selected.size(); i++) {
            if (selected.get(i) instanceof AllColumns) {
                // Handle the case where all columns are selected.
                int tableId = c.getTableId(sb.getFromItem().toString());
                TupleDesc tableDesc = c.getTupleDesc(tableId);
                for (int j = 0; j < tableDesc.numFields(); j++) {
                    names[i] = tableDesc.getFieldName(j);
                }
            } else {
            	SelectExpressionItem item = (SelectExpressionItem) selected.get(i);
            	if(item.getAlias() != null) {
            		names[i] = item.getAlias().getName();
            	}
            	else {
            		ColumnVisitor cv = new ColumnVisitor();
            		cv.visit(item);
            		names[i] = cv.getColumn();
            	}
            }
        }

        // Get the ID and description of the table
        int tableID = c.getTableId(sb.getFromItem().toString());
        TupleDesc td = c.getTupleDesc(tableID);

        // Get types
        Type[] types = new Type[names.length];
        int index = 0;
        for (int i = 0; i < td.numFields(); i++) {
            if (names[index] != null && names[index].equals(td.getFieldName(i))) {
                types[index] = td.getType(i);
                index++;
            }
        }

        // Create a proper TupleDesc with the columns that are needed
        td = new TupleDesc(types, names);

        // Check if the first selected item is AllColumns
        if (selected.get(0) instanceof AllColumns) {
            // Handle the case where all columns are selected.
            int tableId = c.getTableId(sb.getFromItem().toString());
            td = c.getTupleDesc(tableId);
        }

        // Get the list of tuples
        ArrayList<Tuple> tuples = c.getDbFile(tableID).getAllTuples();

        for (int i = 0; i < tuples.size(); i++) {
            tuples.get(i).setDesc(td);
        }
        
     // Apply WHERE condition filtering
        Expression where = sb.getWhere();
        if (where != null) {
            tuples = applyWhereFilter(tuples, where, td);
        }

        Relation r = new Relation(tuples, td);

        // For each join, make a relation
        List<Join> joins = sb.getJoins();
        if (joins != null) {
            for (int i = 0; i < joins.size(); i++) {
                // Get info of joining table
                int otherID = c.getTableId(joins.get(i).getRightItem().toString());
                TupleDesc otherDesc = c.getTupleDesc(otherID);

                ArrayList<Tuple> otherTuples = c.getDbFile(otherID).getAllTuples();
                Relation other = new Relation(otherTuples, otherDesc);

                // Find the info of the join expression
                WhereExpressionVisitor wev = new WhereExpressionVisitor();
                joins.get(i).getOnExpression().accept(wev);

                // Get the left field number
                int left = 0;
                for (int j = 0; j < td.numFields(); j++) {
                    if (wev.getLeft().equals(td.getFieldName(j))) {
                        left = j;
                    }
                }

                // Get right field number
                int right = 0;
                for (int j = 0; j < otherDesc.numFields(); j++) {
                    String s = wev.getRight().toString();
                    s = s.substring(s.indexOf(".") + 1);
                    if (s.equals(otherDesc.getFieldName(j))) {
                        right = j;
                    }
                }

                // Run join
                r = r.join(other, left, right);
            }
        }
        // TODO rework the groupBy and Aggregate for Query
        
        for(int i = 0; i < r.getTuples().size(); i++) {
        	r.getTuples().get(i).setDesc(td);
        }
        
        
        //GROUP AGGREGATE CASE
        List<Expression> groups = sb.getGroupByColumnReferences();
        if(groups != null) {
        	for(int i = 0; i < groups.size(); i++) {
        		
        	}
        }
        
        //NO GROUP AGGREGATE CASE
        else {
        	List<SelectItem> items = sb.getSelectItems();
        	for(SelectItem item : items) {
        		ColumnVisitor cv = new ColumnVisitor();
        		cv.visit((SelectExpressionItem) item);
        		
        		if(cv.getOp() != null) {
        			Aggregator a = new Aggregator(cv.getOp(), false, r.getDesc());
            		
            		for(Tuple t : r.getTuples()) {
            			a.merge(t);
            		}
        		}
        	}
        }
        
        return r;
    }
    
    



    
 // Helper method to apply WHERE condition filtering
    private ArrayList<Tuple> applyWhereFilter(ArrayList<Tuple> tuples, Expression where, TupleDesc td) {
        ArrayList<Tuple> filteredTuples = new ArrayList<>();
        for (Tuple tuple : tuples) {
            if (evaluateWhereExpression(where, tuple, td)) {
                filteredTuples.add(tuple);
            }
        }
        return filteredTuples;
    }
    private boolean evaluateWhereExpression(Expression where, Tuple tuple, TupleDesc td) {
        if (where instanceof EqualsTo) {
            EqualsTo equalsTo = (EqualsTo) where;
            String leftColumn = equalsTo.getLeftExpression().toString();
            String rightValue = equalsTo.getRightExpression().toString();
            int columnIndex = td.nameToId(leftColumn);
            String tupleValue = tuple.getField(columnIndex).toString();
            return tupleValue.equals(rightValue);
        } else if (where instanceof AndExpression) {
            AndExpression andExpression = (AndExpression) where;
            Expression left = andExpression.getLeftExpression();
            Expression right = andExpression.getRightExpression();
            return evaluateWhereExpression(left, tuple, td) && evaluateWhereExpression(right, tuple, td);
        } else if (where instanceof OrExpression) {
            OrExpression orExpression = (OrExpression) where;
            Expression left = orExpression.getLeftExpression();
            Expression right = orExpression.getRightExpression();
            return evaluateWhereExpression(left, tuple, td) || evaluateWhereExpression(right, tuple, td);
        }
        // Handle other expression types as needed
        return false; // Default to false for unsupported expressions
    }



}
