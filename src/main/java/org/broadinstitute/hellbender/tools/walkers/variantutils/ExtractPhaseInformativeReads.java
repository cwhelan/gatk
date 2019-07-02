package org.broadinstitute.hellbender.tools.walkers.variantutils;

import htsjdk.variant.variantcontext.Allele;
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
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import org.broadinstitute.hellbender.utils.variant.GATKVariantContextUtils;
import org.broadinstitute.hellbender.utils.variant.VcfUtils;
import picard.cmdline.programgroups.VariantManipulationProgramGroup;
import scala.Tuple2;

import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;

@CommandLineProgramProperties(
        summary = "This tool extracts phase informative reads for SHAPEIT2 from a 10x VCF",
        oneLineSummary = "Extract phase informative reads from VCF",
        programGroup = VariantManipulationProgramGroup.class
)
@DocumentedFeature
public class ExtractPhaseInformativeReads extends VariantWalker {

    @Argument(
            doc = "Output file",
            fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME
    )
    private String outputFile;

    private Map<String, PhaseSetInfo> phaseSets;
    private Set<String> vcfSamples;
    private PrintWriter outputMapWriter;
    private Map<String, PrintWriter> samplePrintWriters;
    private SortedMap<Integer, List<Allele>> variantMap;

    @Override
    public boolean requiresIntervals() {
        return true;
    }

    @Override
    public void onTraversalStart() {
        // Get list of samples to include in the output
        final Map<String, VCFHeader> vcfHeaders = Collections.singletonMap(getDrivingVariantsFeatureInput().getName(), getHeaderForVariants());
        vcfSamples = VcfUtils.getSortedSampleSet(vcfHeaders, GATKVariantContextUtils.GenotypeMergeType.REQUIRE_UNIQUE);

        phaseSets = new HashMap<>(vcfSamples.size());
        vcfSamples.forEach(s -> phaseSets.put(s, null));

        outputMapWriter = new PrintWriter(BucketUtils.createFile(outputFile + ".map"));

        variantMap = new TreeMap<>();

        samplePrintWriters = vcfSamples.stream().collect(Collectors.toMap(s -> s, s -> new PrintWriter(BucketUtils.createFile(outputFile + "." + s))));

        final long contigCount = intervalArgumentCollection.getIntervals(getSequenceDictionaryForDrivingVariants()).stream().map(SimpleInterval::getContig).count();
        if (contigCount > 1) {
            throw new UserException("Can't run this tool on intervals from more than one contig");
        }

    }

    @Override
    public void apply(final VariantContext variant, final ReadsContext readsContext, final ReferenceContext referenceContext, final FeatureContext featureContext) {
        if (variant.isFiltered()) {
            return;
        }
        if (! (variant.isBiallelic() && variant.isSNP())) {
            return;
        }
        for (final String sample : vcfSamples) {
            final Genotype genotype = variant.getGenotype(sample);

            if (genotype.isHet() && genotype.isPhased()) {
                if (!genotype.hasAnyAttribute("PS")) {
                    logger.warn("Phased variant found with no PS tag at pos " + variant.getStart() + ", genotype = " + genotype.toString());
                    continue;
                }

                initializeVariant(variant, variantMap);

                if (genotype.isPhased()) {
                    final PhaseSetInfo currentSamplePhaseSet = phaseSets.get(sample);
                    final int ps = getPhaseSetId(genotype);
                    final int pq = genotype.hasExtendedAttribute("PQ") ? getPhaseQuality(genotype) : 0;
                    final PhaseSetInfo phaseSetInfo;

                    if (currentSamplePhaseSet == null
                            || currentSamplePhaseSet.getId() != ps) {
                        if (currentSamplePhaseSet != null
                                && currentSamplePhaseSet.getNumSites() > 1) {
                            samplePrintWriters.get(sample).write(currentSamplePhaseSet.toPIRStrings(variantMap));
                        }
                        phaseSetInfo = new PhaseSetInfo(variant.getStart(), ps);
                        phaseSets.put(sample, phaseSetInfo);
                    } else {
                        phaseSetInfo = phaseSets.get(sample);
                    }

                    phaseSetInfo.setNumSites(phaseSetInfo.getNumSites() + 1);
                    phaseSetInfo.setLastObservedLocation(variant.getStart());

                    if (phaseSetInfo.getNumSites() == 1 || phaseSetInfo.getMinPQ() > pq) {
                        phaseSetInfo.setMinPQ(pq);
                    }
                    phaseSetInfo.setTotalPQ(phaseSetInfo.getTotalPQ() + pq);

                    phaseSetInfo.addPhasedGT(variant, genotype);
                }
            }
        }
    }

    private void initializeVariant(final VariantContext variant, final SortedMap<Integer, List<Allele>> variantMap) {
        variantMap.put(variant.getStart(), variant.getAlleles());
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
        outputMapWriter.println("MAP\t" + variantMap.size());
        for (Integer position : variantMap.keySet()) {
            final List<Allele> alleles = variantMap.get(position);
            outputMapWriter.println(position + "\t" + alleles.get(0).getDisplayString() + "\t" + alleles.get(1).getDisplayString());
        }
        outputMapWriter.close();

        for (String sample : phaseSets.keySet()) {
            final PhaseSetInfo currentSamplePhaseSet = phaseSets.get(sample);
            if (currentSamplePhaseSet != null && currentSamplePhaseSet.getNumSites() > 1) {
                samplePrintWriters.get(sample).write(currentSamplePhaseSet.toPIRStrings(variantMap));
                samplePrintWriters.get(sample).close();
            }
        }


        return null;
    }

    static class PhaseSetInfo {
        final int startPos;
        final int id;
        int lastObservedLocation;
        int numSites;

        int minPQ;
        int maxPQ;
        int totalPQ;

        final SortedMap<Integer, Tuple2<Boolean, Integer>> phasedGts = new TreeMap<>();

        public PhaseSetInfo(final int startPos, final int id) {
            this.startPos = startPos;
            this.id = id;
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

        public void addPhasedGT(final VariantContext vc, final Genotype genotype) {
            final Tuple2<Boolean, Integer> value = new Tuple2<>(vc.getAlleleIndex(genotype.getAllele(0)) == 0, Integer.parseInt(genotype.getAnyAttribute("PQ").toString()));
            phasedGts.put(vc.getStart(), value);
        }

        public void setLastObservedLocation(final int lastObservedLocation) {
            this.lastObservedLocation = lastObservedLocation;
        }

        public String toPIRStrings(final Map<Integer, List<Allele>> variantMap) {
            final StringBuffer outString1 = new StringBuffer();
            final StringBuffer outString2 = new StringBuffer();
            boolean first = false;
            phasedGts.forEach((k,v) -> {
                final List<Allele> alleles = variantMap.get(k);
                if (! first) {
                    outString1.append("\t");
                    outString2.append("\t");
                }
                outString1.append(k + "\t" + alleles.get(v._1() ? 0 : 1).getDisplayString() + "\t" + v._2());
                outString2.append(k + "\t" + alleles.get(v._1() ? 1 : 0).getDisplayString() + "\t" + v._2());

            });
            return outString1 + "\n" + outString2 + "\n";
        }
    }
}
