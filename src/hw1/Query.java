package hw1;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Query {

	private String q;
	
	public Query(String q) {
		this.q = q;
	}
	
	public Relation execute()  {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}
		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect)selectStatement.getSelectBody();
		
		
		//your code here
		
		//a catalog to get the tables as needed
		Catalog c = Database.getCatalog();
		
		//which columns does this query want?
		List<SelectItem> selected = sb.getSelectItems();
		
		//first part to building a tuple desc
		String[] names = new String[selected.size()];
		for(int i = 0; i < selected.size(); i++) {
			names[i] = selected.get(i).toString();
		}
		
		//get the id and desc of the table
		int ID = c.getTableId(sb.getFromItem().toString());
		TupleDesc td = c.getTupleDesc(ID);
		
		//get types
		Type[] types = new Type[names.length];
		int index = 0;
		for(int i = 0; i < td.numFields(); i++) {
			if(names[index].equals(td.getFieldName(i))) {
				types[index] = td.getType(i);
				index++;
			}
		}
		
		//now make a proper TupleDesc with the columns that are needed
		td = new TupleDesc(types, names);
		
		//get the list of tuples
		ArrayList<Tuple> tuples = c.getDbFile(ID).getAllTuples();
		
		//build the relation, may need some adjustment
		Relation r = new Relation(tuples, td);
		
		
		return r;
		
	}
}
