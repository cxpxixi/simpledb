package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private static final Field NO_GROUP=new IntField(-1);

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private TupleDesc tupleDesc;
    private Map<Field,Tuple> aggregate;
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
        this.gbfield=gbfield;
        this.gbfieldtype=gbfieldtype;
        this.afield=afield;
        this.what=what;
        aggregate=new HashMap<>();
        if (!what.equals(Op.COUNT)){
            throw new IllegalArgumentException();
        }
        if (gbfield==NO_GROUPING)
        {
            tupleDesc=new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{"aggregateValue"});
            Tuple tuple=new Tuple(tupleDesc);
            tuple.setField(0,new IntField(0));
            aggregate.put(NO_GROUP,tuple);
        }
        else {
            tupleDesc=new TupleDesc(new Type[]{gbfieldtype,Type.INT_TYPE},new String[]{"groupValue", "aggregateValue"});
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        StringField operationField = (StringField) tup.getField(afield);
        if (gbfield==NO_GROUPING){
            Tuple tuple = aggregate.get(NO_GROUP);
            IntField field = (IntField) tuple.getField(0);
            tuple.setField(0,new IntField(field.getValue()+1));
            aggregate.put(NO_GROUP,tuple);
        }
        else {
            Field groupFiled = tup.getField(gbfield);
            if (!aggregate.containsKey(groupFiled)){
                Tuple tuple=new Tuple(tupleDesc);
                tuple.setField(0,groupFiled);
                tuple.setField(1,new IntField(1));
                aggregate.put(groupFiled,tuple);
            }
            else {
                Tuple tuple = aggregate.get(groupFiled);
                IntField field = (IntField) tuple.getField(1);
                tuple.setField(1,new IntField(field.getValue()+1));
                aggregate.put(groupFiled,tuple);
            }
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
//        throw new UnsupportedOperationException("please implement me for lab2");
        return new StringOpIterator(this);
    }

    public class StringOpIterator implements OpIterator{

        private Iterator<Tuple> iterator;
        private StringAggregator aggregator;
        public StringOpIterator(StringAggregator aggregator)
        {
            this.aggregator=aggregator;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            iterator = aggregator.aggregate.values().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return iterator.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            return iterator.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            iterator = aggregator.aggregate.values().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return aggregator.tupleDesc;
        }

        @Override
        public void close() {
            iterator=null;
        }
    }
}
