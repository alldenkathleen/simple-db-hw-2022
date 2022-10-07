package simpledb.execution;

import java.util.ArrayList;
import java.util.HashMap;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> aggregate;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.aggregate = new HashMap<Field,Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        IntField gb;
        if(gbfieldtype==null){
            gb = new IntField(0);
        }
        else{
            gb = (IntField)tup.getField(gbfield);
        }
        if (aggregate.containsKey(gb)){
            if (what.equals(Op.COUNT)){
                int count = aggregate.get(gb)+1;
                aggregate.put(gb, count);
            }
        }
        else{
            if(what.equals(Op.COUNT)){
                aggregate.put(gb, 1);
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *         aggregateVal) if using group, or a single (aggregateVal) if no
     *         grouping. The aggregateVal is determined by the type of
     *         aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        if(gbfieldtype!=null){
            Type[] typeAr = new Type[]{gbfieldtype,Type.INT_TYPE};
            TupleDesc tD = new TupleDesc(typeAr);
            ArrayList<Tuple> tuples = new ArrayList<>();
            for(Field gb: aggregate.keySet()){
                IntField agg = new IntField(aggregate.get(gb));
                Tuple tuple = new Tuple(tD);
                tuple.setField(0, gb);
                tuple.setField(1, agg);
                tuples.add(tuple);
            }
            // System.out.println(tuples.toString());
            TupleIterator tuplesIterator = new TupleIterator(tD, tuples);
            return tuplesIterator;
        }
        else{
            Type[] typeAr = new Type[]{Type.INT_TYPE};
            TupleDesc tD = new TupleDesc(typeAr);
            ArrayList<Tuple> tuples = new ArrayList<>();
            if(aggregate.isEmpty()){
                TupleIterator tuplesIterator = new TupleIterator(tD, tuples);
                return tuplesIterator;
            }
            IntField agg = new IntField(aggregate.get(new IntField(0)));
            Tuple tuple = new Tuple(tD);
            tuple.setField(0, agg);
            tuples.add(tuple);
            TupleIterator tuplesIterator = new TupleIterator(tD, tuples);
            return tuplesIterator;
        }
    }

}
