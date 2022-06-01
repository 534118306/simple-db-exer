package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfieldNum;

    private Type gbfieldtype;

    private int afieldNum;

    private Op what;

    private Map<Field, Integer> groupMap;

    private Map<Field, Integer> countMap;

    private Map<Field, List<Integer>> avgMap;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfieldNum = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afieldNum = afield;
        this.what = what;

        this.groupMap = new HashMap<>();
        this.countMap = new HashMap<>();
        this.avgMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        IntField afield = (IntField) tup.getField(afieldNum);
        Field gbfield = this.gbfieldNum == NO_GROUPING ? null : tup.getField(gbfieldNum);
        int val = afield.getValue();
        if (gbfield != null && gbfield.getType() != this.gbfieldtype) {
            throw new IllegalArgumentException("Given tuple has wrong type");
        }
        // Ops
        switch (this.what) {
            case MIN:
                if(!this.groupMap.containsKey(gbfield)) {
                    this.groupMap.put(gbfield, val);
                } else {
                    this.groupMap.put(gbfield, Math.min(val, this.groupMap.get(gbfield)));
                }
                break;
            case MAX:
                if(!this.groupMap.containsKey(gbfield)) {
                    this.groupMap.put(gbfield, val);
                } else {
                    this.groupMap.put(gbfield, Math.max(val, this.groupMap.get(gbfield)));
                }
                break;
            case SUM:
                if(!this.groupMap.containsKey(gbfield)) {
                    this.groupMap.put(gbfield, val);
                } else {
                    this.groupMap.put(gbfield, this.groupMap.get(gbfield) + val);
                }
                break;
            case COUNT:
                if(!this.groupMap.containsKey(gbfield)) {
                    this.groupMap.put(gbfield, 1);
                } else {
                    this.groupMap.put(gbfield, this.groupMap.get(gbfield) + 1);
                }
                break;
            case AVG:
                if(!this.avgMap.containsKey(gbfield)) {
                    List<Integer> l = new ArrayList<>();
                    l.add(val);
                    avgMap.put(gbfield, l);
                } else {
                    List<Integer> l = avgMap.get(gbfield);
                    l.add(val);
                }
                break;
            case SC_AVG:
            case SUM_COUNT:
            default:
                throw new IllegalArgumentException("Aggregate not supported");
        }
    }


    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new IntAggIterator(groupMap, gbfieldtype);
    }

    private class IntAggIterator extends AggregateIterator {
        private Iterator<Map.Entry<Field, List<Integer>>> avgIterator;
        private boolean isAvg;
        private boolean isSCAvg;
        private boolean isSumCount;

        public IntAggIterator(Map<Field, Integer> groupMap, Type gbFieldType) {
            super(groupMap, gbFieldType);
            this.isAvg = what.equals(Op.AVG);
            this.isSCAvg = what.equals(Op.SC_AVG);
            this.isSumCount = what.equals(Op.SUM_COUNT);
            //SumCount 多一个字段
            if(isSumCount) {
                this.td = new TupleDesc(new Type[]{this.gbFieldType, Type.INT_TYPE, Type.INT_TYPE},
                        new String[]{"groupVal", "sumVal", "countVal"});
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            super.open();
            if(this.isAvg || this.isSumCount) {
                this.avgIterator = avgMap.entrySet().iterator();
            } else {
                this.avgIterator = null;
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(this.isAvg || this.isSumCount) {
                return avgIterator.hasNext();
            } else {
                return super.it.hasNext();
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Tuple tuple = new Tuple(super.td);
            if(this.isAvg || this.isSumCount) {
                Map.Entry<Field, List<Integer>> avgOrSumCountEntry = this.avgIterator.next();
                Field avgOrSumCountField = avgOrSumCountEntry.getKey();
                List<Integer> avgOrSumCountList = avgOrSumCountEntry.getValue();
                if (this.isAvg) {
                    int value = this.listSum(avgOrSumCountList) / avgOrSumCountList.size();
                    setFields(tuple, value, avgOrSumCountField);
                    return tuple;
                } else {
                    setFields(tuple, listSum(avgOrSumCountList), avgOrSumCountField);
                    if(avgOrSumCountField == null) {
                        tuple.setField(1, new IntField(avgOrSumCountList.size()));
                    } else {
                        tuple.setField(2, new IntField(avgOrSumCountList.size()));
                    }
                    return tuple;
                }
            } else if(this.isSCAvg) {
                Map.Entry<Field, Integer> entry = it.next();
                Field f = entry.getKey();
                setFields(tuple, entry.getValue() / countMap.size(), f);
            }
            return super.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            super.rewind();
            if(this.isAvg || this.isSumCount) {
                this.avgIterator = avgMap.entrySet().iterator();
            }
        }

        @Override
        public TupleDesc getTupleDesc() {
            return super.getTupleDesc();
        }

        @Override
        public void close() {
            super.close();
            this.avgIterator = null;
        }

        public int listSum(List<Integer> list) {
            int sum = 0;
            for(int i : list) {
                sum += i;
            }
            return sum;
        }
    }

}
