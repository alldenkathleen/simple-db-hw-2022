package simpledb.execution;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import simpledb.common.Type;
import simpledb.storage.Field;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field,Integer> aggregate;
    private HashMap<Field,Integer> num;
    private HashMap<Field,Integer> den;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.aggregate = new HashMap<Field,Integer>();
        this.num = new HashMap<Field,Integer>();
        this.den = new HashMap<Field,Integer>();
    }

    
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // System.out.println(aggregate.toString());
        Field gb;
        if(gbfieldtype==null){
            gb = new IntField(0);
        }
        else{
            gb = (Field)tup.getField(gbfield);
        }
        if (aggregate.containsKey(gb)){
            IntField agg = (IntField)tup.getField(afield);
            if(what.equals(Op.AVG)){
                int avg = (num.get(gb)+agg.getValue())/(den.get(gb)+1);
                num.put(gb,num.get(gb)+agg.getValue());
                den.put(gb,den.get(gb)+1);
                aggregate.put(gb, avg);
            }
            else if (what.equals(Op.COUNT)){
                int count = aggregate.get(gb)+1;
                aggregate.put(gb, count);
            }
            else if (what.equals(Op.MAX)){
                int currentMax = aggregate.get(gb);
                if(currentMax<agg.getValue()){
                    currentMax = agg.getValue();
                }
                aggregate.put(gb,currentMax);
            }
            else if (what.equals(Op.MIN)){
                int currentMin = aggregate.get(gb);
                if(currentMin>agg.getValue()){
                    currentMin = agg.getValue();
                }
                aggregate.put(gb,currentMin);
            }
            else if (what.equals(Op.SUM)){
                int sum = aggregate.get(gb)+agg.getValue();
                aggregate.put(gb, sum);
            }
        }
        else{
            if(what.equals(Op.COUNT)){
                aggregate.put(gb, 1);
            }
            else{
                IntField agg = (IntField)tup.getField(afield);
                aggregate.put(gb, agg.getValue());
                if(what.equals(Op.AVG)){
                    num.put(gb,agg.getValue());
                    den.put(gb,1);
                }
                // System.out.println(aggregate.toString());
            }
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
            IntField i = new IntField(0);
            IntField agg = new IntField(aggregate.get(i));
            Tuple tuple = new Tuple(tD);
            tuple.setField(0, agg);
            tuples.add(tuple);
            TupleIterator tuplesIterator = new TupleIterator(tD, tuples);
            return tuplesIterator;
        }
    }

}
