package org.broadinstitute.hellbender.tools.spark.sv.utils;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.broadinstitute.hellbender.utils.Utils;
import scala.Tuple2;

import java.util.*;

@DefaultSerializer(PairedStrandedIntervalTree.Serializer.class)
public class PairedStrandedIntervalTree<V> implements Iterable<Tuple2<PairedStrandedIntervals, V>> {

    private final SortedMap<PairedStrandedIntervals, V> map;
    private int maxLeftIntervalLength;

    public PairedStrandedIntervalTree() {
        map = new TreeMap<>();
        maxLeftIntervalLength = 0;
    }

    @SuppressWarnings("unchecked")
    public PairedStrandedIntervalTree( final Kryo kryo, final Input input ) {
        maxLeftIntervalLength = input.readInt();
        map = kryo.readObject(input, TreeMap.class);
    }

    private void serialize( final Kryo kryo, final Output output ) {
        output.writeInt(maxLeftIntervalLength);
        kryo.writeObject(output, map);
    }

    public V put(PairedStrandedIntervals pair, V value) {
        final int leftIntervalLength = pair.getLeft().getInterval().getLength();
        if ( leftIntervalLength > maxLeftIntervalLength ) maxLeftIntervalLength = leftIntervalLength;
        return map.put(pair, value);
    }

    public int size() {
        return map.size();
    }

    public boolean contains( PairedStrandedIntervals pair ) {
        return map.containsKey(pair);
    }

    public Iterator<Tuple2<PairedStrandedIntervals, V>> iterator() {
        return new CompleteIterator<>(map.entrySet().iterator());
    }

    public Iterator<Tuple2<PairedStrandedIntervals,V>> overlappers(PairedStrandedIntervals pair) {
        Utils.nonNull(pair, "Pair to determine overlappers cannot be null.");
        final StrandedInterval left = pair.getLeft();
        final boolean strand = left.getStrand();
        final SVInterval leftInterval = left.getInterval();
        final int contig = leftInterval.getContig();
        // no overlapping pair can start earlier than this
        final int start = Math.max(0, leftInterval.getStart() - maxLeftIntervalLength);
        final StrandedInterval zero = new StrandedInterval(new SVInterval(0,0,0), false);
        final StrandedInterval fromInterval = new StrandedInterval(new SVInterval(contig,start,start), strand);
        final PairedStrandedIntervals fromPair = new PairedStrandedIntervals(fromInterval, zero);
        final int end = leftInterval.getEnd();
        // once we get to here (or greater), we know we're done: we're at or past the end of the left query interval
        final StrandedInterval toInterval = new StrandedInterval(new SVInterval(contig,end,end), strand);
        final PairedStrandedIntervals toPair = new PairedStrandedIntervals(toInterval, zero);
        return new OverlapperIterator<>(map.subMap(fromPair, toPair).entrySet().iterator(), pair);
    }

    private final static class CompleteIterator<V> implements Iterator<Tuple2<PairedStrandedIntervals, V>> {
        private final Iterator<Map.Entry<PairedStrandedIntervals, V>> mapItr;

        CompleteIterator( final Iterator<Map.Entry<PairedStrandedIntervals, V>> mapItr ) {
            this.mapItr = mapItr;
        }

        @Override public boolean hasNext() { return mapItr.hasNext(); }
        @Override public Tuple2<PairedStrandedIntervals, V> next() {
            final Map.Entry<PairedStrandedIntervals, V> entry = mapItr.next();
            return new Tuple2<>(entry.getKey(), entry.getValue());
        }
        @Override public void remove() { mapItr.remove(); }
    }

    private final static class OverlapperIterator<V> implements Iterator<Tuple2<PairedStrandedIntervals, V>> {
        private final Iterator<Map.Entry<PairedStrandedIntervals, V>> mapItr;
        private final PairedStrandedIntervals query;
        private Tuple2<PairedStrandedIntervals, V> next;

        OverlapperIterator( final Iterator<Map.Entry<PairedStrandedIntervals, V>> mapItr,
                                                      final PairedStrandedIntervals query ) {
            this.mapItr = mapItr;
            this.query = query;
            this.next = null;
            hasNext();
        }

        @Override public boolean hasNext() {
            while ( next == null && mapItr.hasNext() ) {
                final Map.Entry<PairedStrandedIntervals, V> test = mapItr.next();
                if ( test.getKey().overlaps(query) ) {
                    next = new Tuple2<>(test.getKey(), test.getValue());
                }
            }
            return next != null;
        }

        @Override public Tuple2<PairedStrandedIntervals, V> next() {
            if ( next == null ) throw new NoSuchElementException("iterator exhausted.");
            final Tuple2<PairedStrandedIntervals, V> result = next;
            next = null;
            return result;
        }

        @Override public void remove() {
            if ( next != null ) throw new IllegalStateException("remove without next.");
            mapItr.remove();
        }
    }

    public static final class Serializer<T> extends com.esotericsoftware.kryo.Serializer<PairedStrandedIntervalTree<T>> {
        @Override
        public void write(final Kryo kryo, final Output output, final PairedStrandedIntervalTree<T> tree ) {
            tree.serialize(kryo, output);
        }

        @Override
        public PairedStrandedIntervalTree<T> read(final Kryo kryo, final Input input, final Class<PairedStrandedIntervalTree<T>> klass ) {
            return new PairedStrandedIntervalTree<>(kryo, input);
        }
    }
}
