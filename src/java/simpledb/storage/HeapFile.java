package simpledb.storage;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private RandomAccessFile randomAccessFile;
    private final File f;
    private final TupleDesc td;
    private int tableId;
    private BufferPool bp;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // TODO: some code goes here
        this.f = f;
        this.td = td;
        this.tableId = this.f.getAbsoluteFile().hashCode();
        try {
            randomAccessFile = new RandomAccessFile(f, "r");
        } catch (Exception e){
            e.printStackTrace();
        }
        Database.getCatalog().addTable(this, f.getName());
        bp =  new BufferPool(this.numPages());
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // TODO: some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // TODO: some code goes here
        return this.tableId; 
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // TODO: some code goes here
        int pgNo = pid.getPageNumber();
        // System.out.println("READ PAGE -- " + pid + "; " + pgNo + "; " + this.numPages());
        if (pgNo >= this.numPages() || pid.getTableId() != this.getId()){
            throw new IllegalArgumentException();
        }
        final int PAGE_SIZE = BufferPool.getPageSize();
        try {
            byte[] slice = new byte[PAGE_SIZE];
            randomAccessFile.seek(pgNo * PAGE_SIZE);
            randomAccessFile.read(slice);
            return new HeapPage(new HeapPageId(this.getId(), pgNo), slice);
        } catch (IOException e) {
            // e.printStackTrace();
            throw new IllegalArgumentException();
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // TODO: some code goes here
        int val = (int) (f.length() / BufferPool.getPageSize());
        if (f.length() % BufferPool.getPageSize() > 0){
            val ++;
        }
        return val;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // TODO: some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // TODO: some code goes here
        HeapFileIterator fileIterator = new HeapFileIterator(tid, this.getId(), this.numPages(), bp);
        return fileIterator;
    }
}
