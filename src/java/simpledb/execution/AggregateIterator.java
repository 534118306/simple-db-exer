package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class AggregateIterator implements OpIterator{

    protected Iterator<Map.Entry<Field, Integer>> it;

    protected TupleDesc td;

    protected Map<Field, Integer> groupMap;

    protected Type gbFieldType;

    public AggregateIterator(Map<Field, Integer> groupMap, Type gbFieldType) {
        this.groupMap = groupMap;
        this.gbFieldType = gbFieldType;
        //没用到group by
        if(this.gbFieldType == null) {
            this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
        } else {
            this.td = new TupleDesc(new Type[]{this.gbFieldType, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});
        }
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        this.it = groupMap.entrySet().iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return it.hasNext();
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        Map.Entry<Field, Integer> entry = it.next();
        Field field = entry.getKey();
        Tuple tuple = new Tuple(this.td);
        this.setFields(tuple, entry.getValue(), field);
        return tuple;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.it = groupMap.entrySet().iterator();
    }

    @Override
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    @Override
    public void close() {
        this.it = null;
        this.td = null;
    }

    public void setFields(Tuple tuple, int value, Field field) {
        if(field == null) {
            tuple.setField(0, new IntField(value));
        } else {
            tuple.setField(0, field);
            tuple.setField(1, new IntField(value));
        }
    }
}
