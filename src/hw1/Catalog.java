package hw1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 */

public class Catalog {
	
    /**
     * Constructor.
     * Creates a new, empty catalog.
     */
	
	//private inner class to hold needed values
	private class table{
		String name;
		HeapFile file;
		String PKey;
		
		public table(HeapFile f, String n, String PK) {
			this.name = n;
			this.file = f;
			this.PKey = PK;
		}
	}
	
	//have a catalog right here
	ArrayList<table> catalog;
	
    public Catalog() {
    	//your code here
    	
    	//initialize the catalog
    	catalog = new ArrayList<table>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified HeapFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(HeapFile file, String name, String pkeyField) {
    	//your code here
    	//create and add a table
    	catalog.add(new table(file, name, pkeyField));
    }

    public void addTable(HeapFile file, String name) {
        addTable(file,name,"");
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) {
    	//your code here
    	
    	//for each entry in the catalog, if the names of the tables are equal get the ID
    	for(int i = 0; i < catalog.size(); i++) {
    		if(catalog.get(i).name.equals(name)) {
    			return catalog.get(i).file.getId();
    		}
    	}
    	//if it cannot be found, throw an exception
    	throw new NoSuchElementException("Element does not exist");
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
    	//your code here
    	
    	//for each table in the catalog, check the IDs, if they are equal get the desc
    	for(int i = 0; i < catalog.size(); i++) {
    		if(catalog.get(i).file.getId() == tableid) {
    			return catalog.get(i).file.getTupleDesc();
    		}
    	}
    	throw new NoSuchElementException("Element does not exist");
    }

    /**
     * Returns the HeapFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the HeapFile.getId()
     *     function passed to addTable
     */
    public HeapFile getDbFile(int tableid) throws NoSuchElementException {
    	//your code here
    	
    	//for each table get the ID, if the IDs match get the file
    	for(int i = 0; i < catalog.size(); i++) {
    		if(catalog.get(i).file.getId() == tableid) {
    			return catalog.get(i).file;
    		}
    	}
    	throw new NoSuchElementException("Element does not exist");
    }

    /** Delete all tables from the catalog */
    public void clear() {
    	catalog.clear();
    }

    public String getPrimaryKey(int tableid) {
    	//your code here
    	
    	//compare IDs and find the PKey
    	for(int i = 0; i < catalog.size(); i++) {
    		if(catalog.get(i).file.getId() == tableid) {
    			return catalog.get(i).PKey;
    		}
    	}
    	throw new NoSuchElementException("Elment does not exist");
    }

    public Iterator<Integer> tableIdIterator() {
    	//your code here
    	
    	//create an arraylist to contain table IDs and return the generated iterator
    	ArrayList<Integer> ids = new ArrayList<Integer>();
    	for(int i = 0; i < catalog.size(); i++) {
    		ids.add(catalog.get(i).file.getId());
    	}
    	return ids.iterator();
    }

    public String getTableName(int id) {
    	//your code here
    	
    	//compare IDs, get the name of the table
    	for(int i = 0; i < catalog.size(); i++) {
    		if(catalog.get(i).file.getId() == id) {
    			return catalog.get(i).name;
    		}
    	}
    	throw new NoSuchElementException("Element does not exist");
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));

            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File("testfiles/" + name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}