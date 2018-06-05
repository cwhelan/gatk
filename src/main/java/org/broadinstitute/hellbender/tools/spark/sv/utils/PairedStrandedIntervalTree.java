package org.broadinstitute.hellbender.tools.spark.sv.utils;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVIntervalTree.Entry;
import org.broadinstitute.hellbender.utils.Utils;
import scala.Tuple2;

import java.util.*;

@DefaultSerializer(PairedStrandedIntervalTree.Serializer.class)
public class PairedStrandedIntervalTree<V> implements Iterable<Tuple2<PairedStrandedIntervals, V>> {

    private final SVIntervalTree<SVIntervalTree<Tuple2<PairedStrandedIntervals, V>>> leftStrandEncodedTree;
    private int size;

    public PairedStrandedIntervalTree() {
        leftStrandEncodedTree = new SVIntervalTree<>();
        size = 0;
    }

    @SuppressWarnings("unchecked")
    public PairedStrandedIntervalTree( final Kryo kryo, final Input input ) {
        size = input.readInt();
        leftStrandEncodedTree =
                (SVIntervalTree<SVIntervalTree<Tuple2<PairedStrandedIntervals, V>>>)kryo.readClassAndObject(input);
    }

    private void serialize( final Kryo kryo, final Output output ) {
        output.writeInt(size);
        kryo.writeClassAndObject(output, leftStrandEncodedTree);
    }

    public V put( PairedStrandedIntervals pair, V value ) {
        final SVInterval leftStrandEncodedInterval = pair.getLeft().getStrandEncodedInterval();
        final Entry<SVIntervalTree<Tuple2<PairedStrandedIntervals, V>>> entry =
                leftStrandEncodedTree.find(leftStrandEncodedInterval);
        final SVIntervalTree<Tuple2<PairedStrandedIntervals, V>> rightStrandEncodedTree;
        if ( entry != null ) {
            rightStrandEncodedTree = entry.getValue();
        } else {
            rightStrandEncodedTree = new SVIntervalTree<>();
            leftStrandEncodedTree.put(leftStrandEncodedInterval, rightStrandEncodedTree);
        }
        final SVInterval rightStrandEncodedInterval = pair.getRight().getStrandEncodedInterval();
        final Tuple2<PairedStrandedIntervals, V> oldVal =
                rightStrandEncodedTree.put(rightStrandEncodedInterval, new Tuple2<>(pair, value));
        size += 1;
        return oldVal == null ? null : oldVal._2();
    }

    public boolean isEmpty() { return leftStrandEncodedTree.size() == 0; }
    public int size() { return size; }

    public boolean contains( PairedStrandedIntervals pair ) {
        final SVInterval leftStrandEncodedInterval = pair.getLeft().getStrandEncodedInterval();
        final SVIntervalTree.Entry<SVIntervalTree<Tuple2<PairedStrandedIntervals, V>>> entry =
                leftStrandEncodedTree.find(leftStrandEncodedInterval);
        if ( entry == null ) return false;
        final SVIntervalTree<Tuple2<PairedStrandedIntervals, V>> rightStrandEncodedTree = entry.getValue();
        final SVInterval rightStrandEncodedInterval = pair.getRight().getStrandEncodedInterval();
        return rightStrandEncodedTree.find(rightStrandEncodedInterval) != null;
    }

    public Iterator<Tuple2<PairedStrandedIntervals, V>> iterator() {
        return new NestedIterator<>(leftStrandEncodedTree.iterator(), null);
    }

    public Iterator<Tuple2<PairedStrandedIntervals,V>> overlappers( PairedStrandedIntervals pair ) {
        final SVInterval leftStrandEncodedInterval = pair.getLeft().getStrandEncodedInterval();
        final SVInterval rightStrandEncodedInterval = pair.getRight().getStrandEncodedInterval();
        return new NestedIterator<>(leftStrandEncodedTree.overlappers(leftStrandEncodedInterval), rightStrandEncodedInterval);
    }

    private final class NestedIterator<V> implements Iterator<Tuple2<PairedStrandedIntervals, V>> {
        private final Iterator<Entry<SVIntervalTree<Tuple2<PairedStrandedIntervals, V>>>> leftIterator;
        private final SVInterval rightStrandEncodedInterval;
        private SVIntervalTree<Tuple2<PairedStrandedIntervals, V>> rightTree;
        private Iterator<Entry<Tuple2<PairedStrandedIntervals, V>>> rightIterator;

        NestedIterator( final Iterator<Entry<SVIntervalTree<Tuple2<PairedStrandedIntervals, V>>>> leftIterator,
                          final SVInterval rightStrandEncodedInterval ) {
            this.leftIterator = leftIterator;
            this.rightStrandEncodedInterval = rightStrandEncodedInterval;
            advance();
        }

        @Override public boolean hasNext() {
            if ( rightIterator == null ) return false;
            if ( !rightIterator.hasNext() ) advance();
            return rightIterator != null;
        }

        @Override public Tuple2<PairedStrandedIntervals, V> next() {
            if ( !hasNext() ) {
                throw new NoSuchElementException("iterator exhausted");
            }
            return rightIterator.next().getValue();
        }

        @Override public void remove() {
            if ( rightIterator == null ) {
                throw new IllegalStateException("remove without next");
            }
            rightIterator.remove();
            if ( rightTree.size() == 0 ) leftIterator.remove();
            PairedStrandedIntervalTree.this.size -= 1;
        }

        private void advance() {
            while ( leftIterator.hasNext() ) {
                rightTree = leftIterator.next().getValue();
                if ( rightStrandEncodedInterval == null ) {
                    rightIterator = rightTree.iterator();
                } else {
                    rightIterator = rightTree.overlappers(rightStrandEncodedInterval);
                }
                if ( rightIterator.hasNext() ) return;
            }
            rightTree = null;
            rightIterator = null;
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
