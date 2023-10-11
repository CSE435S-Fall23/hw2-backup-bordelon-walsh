package hw1;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * A heap file stores a collection of tuples. It is also responsible for managing pages.
 * It needs to be able to manage page creation as well as correctly manipulating pages
 * when tuples are added or deleted.
 * @author Sam Madden modified by Doug Shook
 *
 */
public class HeapFile {
	
	public static final int PAGE_SIZE = 4096;
	
	/**
	 * Creates a new heap file in the given location that can accept tuples of the given type
	 * @param f location of the heap file
	 * @param types type of tuples contained in the file
	 */
	private File file;
	private TupleDesc desc;
	private int numPages;
	
	public HeapFile(File f, TupleDesc type) {
		this.file = f;
		this.desc = type;
		this.numPages = (int) file.length()/PAGE_SIZE;
	}
	
	public File getFile() {
		//your code here
		return file;
	}
	
	public TupleDesc getTupleDesc() {
		//your code here
		return desc;
	}
	
	/**
	 * Creates a HeapPage object representing the page at the given page number.
	 * Because it will be necessary to arbitrarily move around the file, a RandomAccessFile object
	 * should be used here.
	 * @param id the page number to be retrieved
	 * @return a HeapPage at the given page number
	 */
	public HeapPage readPage(int id) {
		//your code here
		try {
			//create an array of bytes
			byte[] bytes = new byte[PAGE_SIZE];
			//open the file
			RandomAccessFile F = new RandomAccessFile(file, "rw");
			long val = id*PAGE_SIZE;
			
			//as long the the value falls within the bounds of the file
			if(val >= 0 && val < file.length()) {
				try {
					//seek the correct position and fill the array
					F.seek(val);
					F.read(bytes);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			//finally, close the file
			try {
				F.close();
				return new HeapPage(id, bytes, getId());
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * Returns a unique id number for this heap file. Consider using
	 * the hash of the File itself.
	 * @return
	 */
	public int getId() {
		//your code here
		//get the hashcode
		return file.getAbsoluteFile().hashCode();
	}
	
	/**
	 * Writes the given HeapPage to disk. Because of the need to seek through the file,
	 * a RandomAccessFile object should be used in this method.
	 * @param p the page to write to disk
	 */
	public void writePage(HeapPage p) {
		//your code here
		try {
			//open the file
			RandomAccessFile F = new RandomAccessFile(file, "rw");
			
			//get the id of the page and find its exact location in the file
			int ID = p.getId();
			long val = ID * PAGE_SIZE;
			
			try {
				//seek the position and write it
				F.seek(val);
				F.write(p.getPageData());
				
				//finally, close the file
				F.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Adds a tuple. This method must first find a page with an open slot, creating a new page
	 * if all others are full. It then passes the tuple to this page to be stored. It then writes
	 * the page to disk (see writePage)
	 * @param t The tuple to be stored
	 * @return The HeapPage that contains the tuple
	 */
	public HeapPage addTuple(Tuple t) {
		//your code here
		
		//make an empty page
		HeapPage page;
		
		//try and find a page with an open slot
		for(int i = 0; i < numPages; i++) {
			page = readPage(i);
			
			//for each slot, check if it is occupied
			for(int j = 0; j < page.getNumSlots(); j++) {
				if(page.slotOccupied(j) == false) {
					try {
						//if there is an open space, add the tuple to it and return
						page.addTuple(t);
						t.setPid(page.getId());
						writePage(page);
						return page;
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}
		
		//if that doesn't work, make an empty page and write it to disk
		byte[] emptyBytes = new byte[PAGE_SIZE];
		try {
			page = new HeapPage(numPages, emptyBytes, getId());
			try {
				//add the tuple to the empty page and increment the number of pages
				page.addTuple(t);
				t.setPid(page.getId());
				writePage(page);
				numPages+=1;
				return page;
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//if none of that works, return null
		return null;
	}
	
	/**
	 * This method will examine the tuple to find out where it is stored, then delete it
	 * from the proper HeapPage. It then writes the modified page to disk.
	 * @param t the Tuple to be deleted
	 */
	
	public void deleteTuple(Tuple t){
		//your code here
		
		//initialize an empty page and find out if it has the tuple
		HeapPage page;
		for(int i = 0; i < numPages; i++) {
			page = readPage(i);
			//if so, delete the tuple
			if(page.getId() == t.getPid()) {
				page.deleteTuple(t);
				writePage(page);
				return;
			}
		}
	}
	
	/**
	 * Returns an ArrayList containing all of the tuples in this HeapFile. It must
	 * access each HeapPage to do this (see iterator() in HeapPage)
	 * @return
	 */
	public ArrayList<Tuple> getAllTuples() {
		//your code here
		
		//create a new arraylist of tuples
		ArrayList<Tuple> tuples = new ArrayList<Tuple>();
		HeapPage page;
		//for each page, get the pages
		for(int i = 0; i < numPages; i++) {
			page = readPage(i);
			Iterator<Tuple> TupleIterator = page.iterator();
			while(TupleIterator.hasNext()) {
				tuples.add(TupleIterator.next());
			}
		}
		return tuples;
	}
	
	/**
	 * Computes and returns the total number of pages contained in this HeapFile
	 * @return the number of pages
	 */
	public int getNumPages() {
		//your code here
		//use the same calculation as above
		this.numPages = (int) file.length()/PAGE_SIZE;
		return numPages;
	}
}