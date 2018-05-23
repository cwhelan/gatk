package org.broadinstitute.hellbender.tools.spark.sv.utils;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import scala.Tuple2;

import java.util.*;

@DefaultSerializer(PairedStrandedIntervalTree.Serializer.class)
public class PairedStrandedIntervalTree<V> implements Iterable<Tuple2<PairedStrandedIntervals, V>> {

    private SVIntervalTree<List<Entry<V>>> leftEnds = new SVIntervalTree<>();

    private int size = 0;

    public PairedStrandedIntervalTree() {}

    @SuppressWarnings("unchecked")
    public PairedStrandedIntervalTree(final Kryo kryo, final Input input) {
        leftEnds = (SVIntervalTree<List<Entry<V>>>) kryo.readClassAndObject(input);
    }

    public V put(PairedStrandedIntervals pair, V value) {
        final SVIntervalTree.Entry<List<Entry<V>>> leftEndEntryList = leftEnds.find(pair.getLeft().getInterval());

        final List<Entry<V>> leftValues;
        if (leftEndEntryList == null) {
            leftValues = new ArrayList<>();
            leftEnds.put(pair.getLeft().getInterval(), leftValues);
        } else {
            leftValues = leftEndEntryList.getValue();
        }

        V oldVal = null;

        for (int i = 0; i < leftValues.size(); i++) {
            Entry<V> entry = leftValues.get(i);
            if (entry.interval.equals(pair)) {
                oldVal = entry.value;
                entry.value = value;
            }
        }


        leftValues.add(new Entry<>(pair, value));
        size = size + 1;
        return oldVal;
    }

    public int size() {
        return size;
    }

    public final class PairedStrandedIntervalTreeOverlapperIterator implements Iterator<Tuple2<PairedStrandedIntervals, V>> {


        private final PairedStrandedIntervals query;
        private Iterator<SVIntervalTree.Entry<List<Entry<V>>>> treeIterator;
        private Iterator<Entry<V>> listIterator;
        private List<Entry<V>> currentList;
        private Entry<V> current;
        private boolean canRemove = false;

        public PairedStrandedIntervalTreeOverlapperIterator(final PairedStrandedIntervalTree<V> tree,
                                                            final PairedStrandedIntervals query) {

            this.query = query;
            treeIterator = tree.leftEnds.overlappers(query.getLeft().getInterval());
            if (treeIterator.hasNext()) {
                final List<Entry<V>> list = treeIterator.next().getValue();
                currentList = list;
                listIterator = list.iterator();
            } else {
                listIterator = Collections.emptyListIterator();
            }
        }

        @Override
        public boolean hasNext() {
            canRemove = false;
            advance();
            return current != null;
        }

        private void advance() {
            current = null;
            while (listIterator.hasNext()) {
                final Entry<V> next = listIterator.next();
                if (next.interval.overlaps(query)) {
                    current = next;
                    return;
                }
            }

            while (treeIterator.hasNext()) {
                final SVIntervalTree.Entry<List<Entry<V>>> nextTreeNode = treeIterator.next();
                currentList = nextTreeNode.getValue();
                listIterator = currentList.iterator();
                while (listIterator.hasNext()) {
                    final Entry<V> next = listIterator.next();
                    if (next.interval.overlaps(query)) {
                        current = next;
                        return;
                    }
                }
            }
        }

        @Override
        public Tuple2<PairedStrandedIntervals, V> next() {
            canRemove = true;
            return new Tuple2<>(current.interval, current.value);
        }

        @Override
        public void remove() {
            if (! canRemove) {
                throw new UnsupportedOperationException("Remove can only be called on this iterator immediately after a call to next. Calling remove after hasNext is unsupported.");
            }
            listIterator.remove();
            if (currentList.isEmpty()) {
                treeIterator.remove();
            }
            canRemove = false;
            size = size - 1;
        }
    }

    public Iterator<Tuple2<PairedStrandedIntervals,V>> overlappers(PairedStrandedIntervals pair) {
        return new PairedStrandedIntervalTreeOverlapperIterator(this, pair);
    }

    public final class PairedStrandedIntervalTreeIterator implements Iterator<Tuple2<PairedStrandedIntervals, V>> {

        private Iterator<SVIntervalTree.Entry<List<Entry<V>>>> treeIterator;
        private Iterator<Entry<V>> listIterator;
        private List<Entry<V>> currentList;

        PairedStrandedIntervalTreeIterator(PairedStrandedIntervalTree<V> tree) {
            treeIterator = tree.leftEnds.iterator();
            if (treeIterator.hasNext()) {
                currentList = treeIterator.next().getValue();
                listIterator = currentList.iterator();
            } else {
                listIterator = Collections.emptyListIterator();
            }
        }

        @Override
        public boolean hasNext() {
            return listIterator.hasNext() || treeIterator.hasNext();
        }


        @Override
        public Tuple2<PairedStrandedIntervals, V> next() {
            if (! listIterator.hasNext()) {
                if (treeIterator.hasNext()) {
                    currentList = treeIterator.next().getValue();
                    listIterator = currentList.iterator();
                }
            }

            final Entry<V> next = listIterator.next();
            return new Tuple2<>(next.interval, next.value);
        }

        @Override
        public void remove() {
            listIterator.remove();
            if (currentList.isEmpty()) {
                treeIterator.remove();
            }
            size = size - 1;
        }

    }

    public Iterator<Tuple2<PairedStrandedIntervals, V>> iterator() {
        return new PairedStrandedIntervalTreeIterator(this);
    }

    public boolean contains(PairedStrandedIntervals pair) {
        final int leftEndIndex = leftEnds.getIndex(pair.getLeft().getInterval());
        if (leftEndIndex == -1) return false;
        final SVIntervalTree.Entry<List<Entry<V>>> leftEndEntry = leftEnds.findByIndex(leftEndIndex);
        final List<Entry<V>> value = leftEndEntry.getValue();

        for (int i = 0; i < value.size(); i++) {
            if (value.get(i).interval.equals(pair)) {
                return true;
            }
        }
        return false;
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


    static final class Entry<V> {
        PairedStrandedIntervals interval;
        V value;

        public Entry(final PairedStrandedIntervals interval, final V value) {
            this.interval = interval;
            this.value = value;
        }
    }

    private void serialize(final Kryo kryo, final Output output) {
        kryo.writeClassAndObject(output, this);
    }
}
