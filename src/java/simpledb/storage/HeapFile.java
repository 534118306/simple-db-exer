package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;

    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
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
        // some code goes here
        int uniqueID = this.file.getAbsoluteFile().hashCode();
        return uniqueID;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int tableID = pid.getTableId();
        int pageNumber = pid.getPageNumber();

        RandomAccessFile accessFile = null;

        try {
            accessFile = new RandomAccessFile(this.file, "r");
            if((pageNumber + 1) * BufferPool.getPageSize() > accessFile.length()) {
                accessFile.close();
                throw new IllegalArgumentException("No match page in HeapFile");
            }

            byte[] bytes = new byte[BufferPool.getPageSize()];
            accessFile.seek(pageNumber * BufferPool.getPageSize());
            int len = accessFile.read(bytes, 0, BufferPool.getPageSize());
            if(len != BufferPool.getPageSize()) {
                throw new IllegalArgumentException(String.format("table %d page %d read %d bytes", tableID, pageNumber, len));
            }
            HeapPageId pageId = new HeapPageId(tableID, pageNumber);
            HeapPage page = new HeapPage(pageId, bytes);
            return page;
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                accessFile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableID, pageNumber));
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageNumber = page.getId().getPageNumber();
        if(pageNumber > numPages()) {
            throw new IllegalArgumentException("page is not in the HeapFile or pageId is wrong");
        }
        RandomAccessFile accessFile = new RandomAccessFile(this.file, "rw");
        accessFile.seek(pageNumber * BufferPool.getPageSize());
        byte[] bytes = page.getPageData();
        accessFile.write(bytes);
        accessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        long fileLen = this.file.length();
        int numPages = (int)Math.floor(fileLen * 1.0 / BufferPool.getPageSize());
        return numPages;
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> list = new ArrayList<>();
        BufferPool pool = Database.getBufferPool();
        int tableid = getId();
        for(int i = 0; i < numPages(); i ++) {
            PageId pid = new HeapPageId(tableid, i);
            HeapPage page = (HeapPage) pool.getPage(tid, pid, Permissions.READ_WRITE);
            //该页没有空slot时释放该page上的锁
            if(page.getNumEmptySlots() == 0) {
                Database.getBufferPool().unsafeReleasePage(tid, pid);
                continue;
            }
            if(page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                page.markDirty(true, tid);
                list.add(page);
                return list;
            }
        }
        //需要创建新的页去插入tuple
        HeapPage page = new HeapPage(new HeapPageId(tableid, numPages()), HeapPage.createEmptyPageData());
        page.insertTuple(t);
        writePage(page);
        list.add(page);
        return list;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> list = new ArrayList<>();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        list.add(page);
        return list;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        DbFileIterator it = new HeapFileIterator(this, tid);
        return it;
    }

    private static final class HeapFileIterator implements DbFileIterator {
        private final HeapFile heapFile;

        private final TransactionId tid;

        private Iterator<Tuple> iterator;

        private int pageNum;

        public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
            this.heapFile = heapFile;
            this.tid = tid;
        }


        /**
         * Onpens an iterator
         * @throws DbException
         * @throws TransactionAbortedException
         */
        @Override
        public void open() throws DbException, TransactionAbortedException {
            pageNum = 0;
            iterator = getPageTuples(pageNum);
        }

        /**
         * 注意其他位置获取page时，均需要BufferPool中的getPage()方法调用
         * @param pageNumber
         * @return
         * @throws DbException
         * @throws TransactionAbortedException
         */
        private Iterator<Tuple> getPageTuples(int pageNumber) throws DbException, TransactionAbortedException {
            if(pageNumber >= 0 && pageNumber < heapFile.numPages()) {
                HeapPageId pid = new HeapPageId(heapFile.getId(), pageNumber);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            } else {
                throw new DbException(String.format("HeapFile %d dosen't contain page %d !", heapFile.getId(), pageNumber));
            }
        }

        /**
         * true if there are more tuples available, false if no more tuples or iterator isn't open
         * @return
         * @throws DbException
         * @throws TransactionAbortedException
         */
        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(!iterator.hasNext()){
                while(pageNum < heapFile.numPages() - 1) {
                    pageNum ++;
                    iterator = getPageTuples(pageNum);
                    if(iterator.hasNext()) {
                        return iterator.hasNext();
                    }
                }
                if(pageNum >= heapFile.numPages() - 1) {
                    return false;
                }
            } else {
                return true;
            }
            return false;
        }

        /**
         * Gets the next tuple from the operator (typically implementing by reading
         * from a child operator or an access method).
         * @return
         * @throws DbException
         * @throws TransactionAbortedException
         * @throws NoSuchElementException
         */
        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(iterator == null || !iterator.hasNext()) {
                throw new NoSuchElementException();
            }
            return iterator.next();
        }

        /**
         * Resets the iterator to the start.
         * @throws DbException
         * @throws TransactionAbortedException
         */
        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        /**
         * close the Iterator
         */
        @Override
        public void close() {
            iterator = null;
        }
    }

}

