package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;

import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfieldNum;

    private Type gbfieldtype;

    private int afieldNum;

    private Op what;

    private Map<Field, Integer> groupMap;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfieldNum = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afieldNum = afield;
        this.what = what;

        this.groupMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        StringField afield = (StringField) tup.getField(afieldNum);
        Field gbfield  = tup.getField(gbfieldNum);
        String val = afield.getValue();
        if(gbfield != null && gbfield.getType() != gbfieldtype) {
            throw new IllegalArgumentException("Given tuple has wrong type");
        }
        if(!this.groupMap.containsKey(gbfield)) {
            this.groupMap.put(gbfield, 1);
        } else {
            this.groupMap.put(gbfield, this.groupMap.get(gbfield) + 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new StringAggIterator(this.groupMap, this.gbfieldtype);
    }

    private class StringAggIterator extends AggregateIterator {
        public StringAggIterator(Map<Field, Integer> groupMap, Type gbFieldType) {
            super(groupMap, gbFieldType);
        }
    }
}
