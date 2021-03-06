package org.broadinstitute.hellbender.tools.spark.linkedreads;

import htsjdk.samtools.CigarElement;
import htsjdk.samtools.CigarOperator;
import htsjdk.samtools.SAMFileHeader;
import htsjdk.samtools.filter.OverclippedReadFilter;
import org.broadinstitute.hellbender.engine.filters.ReadFilter;
import org.broadinstitute.hellbender.engine.filters.ReadFilterLibrary;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.io.Serializable;
import java.util.function.Predicate;

public class LinkedReadAnalysisFilter extends ReadFilter implements Serializable {
    private static final long serialVersionUID = 1l;

    final Predicate<GATKRead> filter;

    public LinkedReadAnalysisFilter() {
        final Predicate<GATKRead> readPredicate = ReadFilterLibrary.MAPPED
                .and(ReadFilterLibrary.PASSES_VENDOR_QUALITY_CHECK)
                .and(ReadFilterLibrary.NOT_DUPLICATE)
                .and(ReadFilterLibrary.PRIMARY_LINE)
                .and(new BarcodedReadFilter())
                .and(new OverclippedReadFilter());
        this.filter = readPredicate;
    }

    @Override
    public boolean test(final GATKRead read) {
        return filter.test(read);
    }

    public static class BarcodedReadFilter extends ReadFilter {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean test(final GATKRead read) {
            return read.hasAttribute("BX");
        }
    }

    public static class OverclippedReadFilter extends ReadFilter {
        private static final long serialVersionUID = 1L;

        @Override
        public boolean test(final GATKRead read) {
            return !filterOut(read, 36, true);
        }

        // Stolen from htsjdk.samtools.filter.OverclippedReadFilter so that I can use GATKRead
        public boolean filterOut(final GATKRead record, final int unclippedBasesThreshold, final boolean filterSingleEndClips) {
            int alignedLength = 0;
            int softClipBlocks = 0;
            int minSoftClipBlocks = filterSingleEndClips ? 1 : 2;
            CigarOperator lastOperator = null;

            for ( final CigarElement element : record.getCigar().getCigarElements() ) {
                if ( element.getOperator() == CigarOperator.S ) {
                    //Treat consecutive S blocks as a single one
                    if(lastOperator != CigarOperator.S){
                        softClipBlocks += 1;
                    }

                } else if ( element.getOperator().consumesReadBases() ) {   // M, I, X, and EQ (S was already accounted for above)
                    alignedLength += element.getLength();
                }
                lastOperator = element.getOperator();
            }

            return(alignedLength < unclippedBasesThreshold && softClipBlocks >= minSoftClipBlocks);
        }

    }

}
