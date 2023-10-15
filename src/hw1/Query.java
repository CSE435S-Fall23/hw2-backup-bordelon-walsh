package hw1;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
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
            } else if (selected.get(i) instanceof SelectExpressionItem) {
                SelectExpressionItem item = (SelectExpressionItem) selected.get(i);
                if (item.getAlias() != null) {
                    names[i] = item.getAlias().getName();
                } else {
                    if (item.getExpression() instanceof Column) {
                        Column column = (Column) item.getExpression();
                        names[i] = column.getColumnName();
                    }
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
        //TODO still need to do where and join

        // Get the list of tuples
        ArrayList<Tuple> tuples = c.getDbFile(tableID).getAllTuples();

        return new Relation(tuples, td);


    }
}
