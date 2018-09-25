package org.broadinstitute.hellbender.tools.spark.linkedreads;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.util.Map;

/**
 * A lightweight object to summarize reads for the purposes of collecting linked read information
 */
@DefaultSerializer(ReadInfo.Serializer.class)
class ReadInfo {
    ReadInfo(final Map<String, Integer> contigNameMap, final GATKRead gatkRead) {
        this.contig = contigNameMap.get(gatkRead.getContig());
        this.start = gatkRead.getStart();
        this.end = gatkRead.getEnd();
        this.forward = !gatkRead.isReverseStrand();
        this.mapq = gatkRead.getMappingQuality();
    }

    ReadInfo(final int contig, final int start, final int end, final boolean forward, final int mapq) {
        this.contig = contig;
        this.start = start;
        this.end = end;
        this.forward = forward;
        this.mapq = mapq;
    }

    int contig;
    int start;
    int end;
    boolean forward;
    int mapq;

    public ReadInfo(final Kryo kryo, final Input input) {
        contig = input.readInt();
        start = input.readInt();
        end = input.readInt();
        forward = input.readBoolean();
        mapq = input.readInt();
    }

    public int getContig() {
        return contig;
    }

    public int getStart() {
        return start;
    }

    public int getEnd() {
        return end;
    }

    public boolean isForward() {
        return forward;
    }

    public int getMapq() {
        return mapq;
    }

    public static final class Serializer extends com.esotericsoftware.kryo.Serializer<ReadInfo> {
        @Override
        public void write(final Kryo kryo, final Output output, final ReadInfo interval ) {
            interval.serialize(kryo, output);
        }

        @Override
        public ReadInfo read(final Kryo kryo, final Input input, final Class<ReadInfo> klass ) {
            return new ReadInfo(kryo, input);
        }
    }

    private void serialize(final Kryo kryo, final Output output) {
        output.writeInt(contig);
        output.writeInt(start);
        output.writeInt(end);
        output.writeBoolean(forward);
        output.writeInt(mapq);
    }

    @Override
    public String toString() {
        return "ReadInfo{" +
                "contig=" + contig +
                ", start=" + start +
                ", end=" + end +
                ", forward=" + forward +
                ", mapq=" + mapq +
                '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final ReadInfo readInfo = (ReadInfo) o;

        if (contig != readInfo.contig) return false;
        if (start != readInfo.start) return false;
        if (end != readInfo.end) return false;
        if (forward != readInfo.forward) return false;
        return mapq == readInfo.mapq;
    }

    @Override
    public int hashCode() {
        int result = contig;
        result = 31 * result + start;
        result = 31 * result + end;
        result = 31 * result + (forward ? 1 : 0);
        result = 31 * result + mapq;
        return result;
    }
}
