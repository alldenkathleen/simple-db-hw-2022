package simpledb.storage;

import java.io.DataInputStream;
import java.nio.Buffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

public class HeapFileIterator extends AbstractDbFileIterator{
    
    private BufferPool bufferPool;
    private TransactionId tid;
    private Iterator<Tuple> tupleIterator;
    private int pageNo = 0;
    private int numPages;
    private int tableId;
    public HeapFileIterator(TransactionId tid, int tableId,int numPages,BufferPool bp){
        this.bufferPool = bp;
        this.tid = tid;
        this.pageNo = numPages;
        this.tupleIterator = null;
        this.numPages = numPages;
        this.tableId = tableId;
    }
    public void open() throws DbException, TransactionAbortedException{
        pageNo = 0;
        tupleIterator = null;
    }

    /**
     * Resets the iterator to the start.
     *
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException{
        pageNo = 0;
        tupleIterator = null;
    }
    public void close(){
        pageNo = numPages;
        tupleIterator = null;
        super.close();
    }
    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        while(this.pageNo <= this.numPages) {
            if (tupleIterator != null && tupleIterator.hasNext()){
                Tuple tuple = tupleIterator.next();
                return tuple;
            }
            if (pageNo == this.numPages){
                break;
            }
            HeapPage page = (HeapPage)bufferPool.getPage(tid, new HeapPageId(tableId, pageNo), Permissions.READ_ONLY);
            this.pageNo ++;
            if (page != null){
                tupleIterator = page.iterator();
            } else {
                tupleIterator = null;
            }
        }
        return null;
    }
}
