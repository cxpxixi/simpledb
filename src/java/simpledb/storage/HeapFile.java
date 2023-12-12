package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

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

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private File file;
    private TupleDesc td;
    private BufferPool bufferPool;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file=f;
        this.td=td;
        this.bufferPool=Database.getBufferPool();
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
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
//        throw new UnsupportedOperationException("implement this");
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int pageSize = BufferPool.getPageSize();
        int pageNumber = pid.getPageNumber();
        int offset=pageSize*pageNumber;
        Page page=null;
        RandomAccessFile randomAccessFile = null;
        try {
            randomAccessFile=new RandomAccessFile(file,"r");
            byte[] data = new byte[pageSize];
            randomAccessFile.seek(offset);
            randomAccessFile.read(data);
            page = new HeapPage(((HeapPageId) pid), data);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                randomAccessFile.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageSize=BufferPool.getPageSize();
        int pageNumber = page.getId().getPageNumber();
        int offset=pageSize*pageNumber;
        RandomAccessFile randomAccessFile = null;
        randomAccessFile=new RandomAccessFile(file,"rw");
        randomAccessFile.seek(offset);
        randomAccessFile.write(page.getPageData());
        randomAccessFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        long length = this.file.length();
        return ((int) Math.ceil(length * 1.0 / BufferPool.getPageSize()));
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> modified = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            HeapPage page = (HeapPage) bufferPool.getPage(tid, new HeapPageId(getId(),i),Permissions.READ_WRITE);
            if (page.getNumEmptySlots()==0){
                bufferPool.unsafeReleasePage(tid, page.getId());
                continue;
            }
            page.insertTuple(t);
            modified.add(page);
            return modified;
        }
        // 当所有的页都满时,我们需要创建新的页并写入文件中
        BufferedOutputStream outputStream = new BufferedOutputStream(new FileOutputStream(file, true));
        byte[] emptyPageData = HeapPage.createEmptyPageData();
        // 向文件末尾添加数据
        outputStream.write(emptyPageData);
        outputStream.close();
        // 加载到缓存中,使用numPages() - 1是因为此时numPages()已经变为插入后的大小了
        HeapPage page = (HeapPage) bufferPool.getPage(tid, new HeapPageId(getId(), numPages() - 1), Permissions.READ_WRITE);
        page.insertTuple(t);
        modified.add(page);
        return modified;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapPage page = (HeapPage) bufferPool.getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        ArrayList<Page> modified = new ArrayList<>();
        modified.add(page);
        return modified;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this,tid);
    }

    public class HeapFileIterator implements DbFileIterator{
        private TransactionId tid;
        private HeapFile heapFile;
        private Iterator<Tuple> iterator;
        private int pageNumber;
        public HeapFileIterator(HeapFile heapFile,TransactionId tid)
        {
            this.tid=tid;
            this.heapFile=heapFile;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.pageNumber=0;
            this.iterator=getPageTuples(pageNumber);
        }

        private Iterator<Tuple> getPageTuples(int pageNo) throws TransactionAbortedException, DbException{
            if (pageNo>=0&&pageNo<numPages())
            {
                HeapPageId heapPageId=new HeapPageId(heapFile.getId(), pageNo);
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, heapPageId, Permissions.READ_ONLY);
                return page.iterator();
            }
            else{
                throw new DbException("");
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (iterator==null)
            {
                return false;
            }
            while (iterator!=null&&!iterator.hasNext())
            {
                if (pageNumber<numPages()-1){
                    pageNumber++;
                    iterator=getPageTuples(pageNumber);
                }
                else {
                    return false;
                }
            }
            return iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (iterator == null || !iterator.hasNext()) {
                throw new NoSuchElementException();
            }
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            iterator=null;
        }
    }
}

