package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * 对Predicate封装，通过predicate实现对每一个tuple的过滤操作
     */
    private final Predicate predicate;

    private OpIterator opIterator;

    private final TupleDesc tupleDesc;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        // some code goes here
        this.predicate = p;
        this.opIterator = child;
        this.tupleDesc = child.getTupleDesc();
    }

    public Predicate getPredicate() {
        // some code goes here
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleDesc;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        opIterator.open();
    }

    public void close() {
        // some code goes here
        super.close();
        opIterator.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        opIterator.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if(opIterator == null) {
            throw new NoSuchElementException("Operator Iterator is empty");
        }
        while (opIterator.hasNext()) {
            Tuple tuple = opIterator.next();
            if(predicate.filter(tuple)) return tuple;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[] opIterators = new OpIterator[]{this.opIterator};
        return opIterators;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        if(children == null || children.length == 0) return;
        this.opIterator  = children[0];
    }

}
