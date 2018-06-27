package org.broadinstitute.hellbender.tools.walkers.variantutils;

import htsjdk.variant.variantcontext.Genotype;
import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFHeader;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.engine.FeatureContext;
import org.broadinstitute.hellbender.engine.ReadsContext;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.engine.VariantWalker;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import org.broadinstitute.hellbender.utils.variant.GATKVariantContextUtils;
import org.broadinstitute.hellbender.utils.variant.VcfUtils;
import picard.cmdline.programgroups.VariantManipulationProgramGroup;

import java.io.PrintWriter;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@CommandLineProgramProperties(
        summary = "This tool extracts the boundaries of phase sets from a VCF and writes them to a BED file",
        oneLineSummary = "Extract phase sets from VCF",
        programGroup = VariantManipulationProgramGroup.class
)
@DocumentedFeature
public class ExtractPhaseSets extends VariantWalker {

    @Argument(
            doc = "Output file for sample phase sets",
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME
    )
    private String outputFile;

    @Argument(
            doc = "Output file for het site report",
            fullName = "hetSiteReport",
            optional = true
    )
    private String hetSiteReport;

    private Map<String, PhaseSetInfo> phaseSets;
    private Set<String> vcfSamples;
    private PrintWriter output;
    private PrintWriter hetSiteWriter;

    @Override
    public void onTraversalStart() {
        // Get list of samples to include in the output
        final Map<String, VCFHeader> vcfHeaders = Collections.singletonMap(getDrivingVariantsFeatureInput().getName(), getHeaderForVariants());
        vcfSamples = VcfUtils.getSortedSampleSet(vcfHeaders, GATKVariantContextUtils.GenotypeMergeType.REQUIRE_UNIQUE);

        phaseSets = new HashMap<>(vcfSamples.size());
        vcfSamples.forEach(s -> phaseSets.put(s, null));

        output = new PrintWriter(BucketUtils.createFile(outputFile));
        output.print("CHROM\tSTART\tEND\tID\tNUM_SITES\tMIN_PQ\tMEAN_PQ\tMAX_PQ\tSAMPLE\n");

        if (hetSiteReport != null) {
            hetSiteWriter = new PrintWriter(BucketUtils.createFile(hetSiteReport));
            hetSiteWriter.print("CHROM\tPOS\tPHASED\tPQ\tSAMPLE\n");
        }

    }

    @Override
    public void apply(final VariantContext variant, final ReadsContext readsContext, final ReferenceContext referenceContext, final FeatureContext featureContext) {
        if (variant.isFiltered()) {
            return;
        }
        for (final String sample : vcfSamples) {
            final Genotype genotype = variant.getGenotype(sample);
            if (genotype.isHet()) {
                if (genotype.isPhased() && !genotype.hasAnyAttribute("PS")) {
                    logger.warn("Phased variant found with no PS tag at pos " + variant.getStart() + ", genotype = " + genotype.toString());
                    continue;
                }

                if (hetSiteWriter != null) {
                    final int pq;
                    if (genotype.isPhased() && genotype.hasExtendedAttribute("PQ")) {
                        pq = genotype.hasExtendedAttribute("PQ") ? getPhaseQuality(genotype) : 0;
                    } else {
                        pq = 0;
                    }
                    hetSiteWriter.print(variant.getContig() + "\t" + variant.getStart() + "\t" + genotype.isPhased() + "\t" + pq + "\t" + sample + "\n");
                }
                if (genotype.isPhased()) {
                    final PhaseSetInfo currentSamplePhaseSet = phaseSets.get(sample);
                    final int ps = getPhaseSetId(genotype);
                    final int pq = genotype.hasExtendedAttribute("PQ") ? getPhaseQuality(genotype) : 0;
                    final PhaseSetInfo phaseSetInfo;

                    if (currentSamplePhaseSet == null
                            || !currentSamplePhaseSet.getContig().equals(variant.getContig())
                            || currentSamplePhaseSet.getId() != ps) {
                        if (currentSamplePhaseSet != null
                                && currentSamplePhaseSet.getNumSites() > 1) {
                            output.print(currentSamplePhaseSet.toBedString() + "\t" + sample + "\n");
                        }
                        phaseSetInfo = new PhaseSetInfo(variant.getContig(), variant.getStart(), ps);
                        phaseSets.put(sample, phaseSetInfo);
                    } else {
                        phaseSetInfo = phaseSets.get(sample);
                    }

                    phaseSetInfo.setNumSites(phaseSetInfo.getNumSites() + 1);
                    phaseSetInfo.setLastObservedLocation(variant.getStart());

                    if (phaseSetInfo.getNumSites() == 1 || phaseSetInfo.getMinPQ() > pq) {
                        phaseSetInfo.setMinPQ(pq);
                    }
                    if (phaseSetInfo.getNumSites() == 1 || phaseSetInfo.getMaxPQ() < pq) {
                        phaseSetInfo.setMaxPQ(pq);
                    }
                    phaseSetInfo.setTotalPQ(phaseSetInfo.getTotalPQ() + pq);
                }
            }
        }
    }

    private int getPhaseSetId(final Genotype genotype) {
        return getGenotypeAttributeAsInt(genotype, "PS");
    }

    private int getPhaseQuality(final Genotype genotype) {
        return getGenotypeAttributeAsInt(genotype, "PQ");
    }

    private int getGenotypeAttributeAsInt(final Genotype genotype, final String tag) {
        Object x = genotype.getExtendedAttribute(tag);
        if ( x == null || x == VCFConstants.MISSING_VALUE_v4 ) return 0;
        if ( x instanceof Integer ) return (Integer)x;
        return Integer.valueOf((String)x); // throws an exception if this isn't a string
    }

    @Override
    public Object onTraversalSuccess() {
        for (String sample : phaseSets.keySet()) {
            final PhaseSetInfo currentSamplePhaseSet = phaseSets.get(sample);
            if (currentSamplePhaseSet != null && currentSamplePhaseSet.getNumSites() > 1) {
                output.print(currentSamplePhaseSet.toBedString() + "\t" + sample + "\n");
            }
        }

        output.close();
        if (hetSiteWriter != null) {
            hetSiteWriter.close();
        }
        return null;
    }

    static class PhaseSetInfo {
        final String contig;
        final int startPos;
        final int id;
        int lastObservedLocation;
        int numSites;

        int minPQ;
        int maxPQ;
        int totalPQ;

        public PhaseSetInfo(final String contig, final int startPos, final int id) {
            this.contig = contig;
            this.startPos = startPos;
            this.id = id;
        }

        public String getContig() {
            return contig;
        }

        public int getStartPos() {
            return startPos;
        }

        public int getId() {
            return id;
        }

        public int getNumSites() {
            return numSites;
        }

        public void setNumSites(final int numSites) {
            this.numSites = numSites;
        }

        public int getMinPQ() {
            return minPQ;
        }

        public void setMinPQ(final int minPQ) {
            this.minPQ = minPQ;
        }

        public int getMaxPQ() {
            return maxPQ;
        }

        public void setMaxPQ(final int maxPQ) {
            this.maxPQ = maxPQ;
        }

        public int getTotalPQ() {
            return totalPQ;
        }

        public void setTotalPQ(final int totalPQ) {
            this.totalPQ = totalPQ;
        }

        public int getLastObservedLocation() {
            return lastObservedLocation;
        }

        public void setLastObservedLocation(final int lastObservedLocation) {
            this.lastObservedLocation = lastObservedLocation;
        }

        public String toBedString() {
            return contig + "\t" +  startPos + "\t" + lastObservedLocation + "\t" + id + "\t" + numSites + "\t" + minPQ + "\t" + (totalPQ / numSites) + "\t" + maxPQ;
        }
    }
}
