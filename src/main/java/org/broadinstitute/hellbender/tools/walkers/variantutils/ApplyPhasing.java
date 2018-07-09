package org.broadinstitute.hellbender.tools.walkers.variantutils;

import htsjdk.variant.variantcontext.*;
import htsjdk.variant.variantcontext.writer.VariantContextWriter;
import htsjdk.variant.vcf.VCFConstants;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLine;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.engine.*;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import org.broadinstitute.hellbender.utils.variant.GATKVariantContextUtils;
import picard.cmdline.programgroups.VariantManipulationProgramGroup;

import java.io.File;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.Collectors;

@CommandLineProgramProperties(
        summary = "This tool applies the phasing from one VCF to the genotypes in a second, producing reports on cases where genotypes are incompatible or sites are missing",
        oneLineSummary = "Apply Phasing to VCF",
        programGroup = VariantManipulationProgramGroup.class
)
@DocumentedFeature
public class ApplyPhasing extends VariantWalker {

    @Argument(fullName="phased-variants-file", doc="the phased VCF file", optional=false)
    private List<FeatureInput<VariantContext>> phased;

    @Argument(doc="File to which variants should be written", fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME, shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME, optional = false)
    public File out = null;

    @Argument(doc="Additional genotype format tags to apply", fullName = "additional-tags", optional = true)
    public List<String> additionalTags = null;

    @Argument(doc="Missed alleles report", fullName = "missing-alleles-report", optional = true)
    public String missingAllelesReport = null;

    @Argument(doc="Concordance summary report", fullName = "concordance-summary-report", optional = true)
    public String concordanceSummaryReport = null;

    @Argument(doc="Discordant genotypes report", fullName = "discordant-genotypes-report", optional = true)
    public String discordantGenotypesReport = null;

    private VariantContextWriter vcfWriter;
    private PrintWriter missingAllelesReportWriter;
    private PrintWriter concordanceSummaryReportWriter;
    private PrintWriter discordantGenotypesReportWriter;
    private ConcordanceSummary concordanceSummary;

    @Override
    public void onTraversalStart() {
        vcfWriter = createVCFWriter(out);

        final VCFHeader header = getHeaderForVariants();

        if ( ! header.hasGenotypingData() ) {
            throw new UserException.BadInput("VCF has no genotypes");
        }

        final Set<VCFHeaderLine> headerLines = new TreeSet<>(header.getMetaDataInSortedOrder());
        final Set<String> tagsToFind = new HashSet<>();
        tagsToFind.add(VCFConstants.PHASE_SET_KEY);
        tagsToFind.add(VCFConstants.PHASE_QUALITY_KEY);
        tagsToFind.addAll(additionalTags);

        for (final FeatureInput<VariantContext> phasedVariantInput : phased) {
            final VCFHeader headerForFeatures = (VCFHeader) getHeaderForFeatures(phasedVariantInput);
            for (Iterator<String> tagIterator = tagsToFind.iterator(); tagIterator.hasNext(); ) {
                String tag = tagIterator.next();
                if (headerForFeatures.hasFormatLine(tag)) {
                    headerLines.add(headerForFeatures.getFormatHeaderLine(tag));
                    tagIterator.remove();
                }
            }
            if (tagsToFind.isEmpty()) break;
        }

        if (! tagsToFind.isEmpty()) {
            throw new GATKException("Phased variant file does not contain a header line for requested genotype tags " + tagsToFind);
        }

        vcfWriter.writeHeader(new VCFHeader(headerLines, header.getGenotypeSamples()));

        if (missingAllelesReport != null) {
            missingAllelesReportWriter = new PrintWriter(BucketUtils.createFile(missingAllelesReport));
            missingAllelesReportWriter.print("CHROM\tPOS\tJOINT_ALLELES\tSAMPLE\tSAMPLE_ALLELES\n");
        }

        if (concordanceSummaryReport != null) {
            concordanceSummaryReportWriter = new PrintWriter(BucketUtils.createFile(concordanceSummaryReport));
        }

        if (discordantGenotypesReport != null) {
            discordantGenotypesReportWriter = new PrintWriter(BucketUtils.createFile(discordantGenotypesReport));
            discordantGenotypesReportWriter.print("CHROM\tPOS\tJOINT_TYPE\tJOINT_QUAL\tPHASED_QUAL\tHAPLOCALLED\tSAMPLE\tJOINT_GT\t\tJOINT_GQ\tJOINT_DP\tPHASED_GT\tPHASED_GQ\tPHASED_DP\n");
        }

        concordanceSummary = new ConcordanceSummary();
    }

    @Override
    public void apply(final VariantContext variant,
                      final ReadsContext readsContext,
                      final ReferenceContext referenceContext,
                      final FeatureContext featureContext) {
        final List<VariantContext> phasedOverlappingVariants = featureContext.getValues(phased);
        final List<VariantContext> phasedVariantsWithSameStart =
                phasedOverlappingVariants.stream().filter(v -> v.getStart() == variant.getStart()).collect(Collectors.toList());

        concordanceSummary.sawVariant(variant);

        if (phasedVariantsWithSameStart.size() == 0) {
            vcfWriter.add(variant);
            return;
        }

        VariantContextBuilder newVariantBuilder = new VariantContextBuilder(variant);
        final GenotypesContext genotypesContext = GenotypesContext.copy(variant.getGenotypes());

//        newVariantBuilder.ge

        boolean foundMatch = false;
        for (final VariantContext phasedVariant : phasedVariantsWithSameStart) {
            if (phasedVariant.isFiltered()) {
                continue;
            }

            if (! foundMatch) {
                concordanceSummary.variantSiteMatch(variant);
                foundMatch = true;
            }


            final boolean phasedVariantRefLonger = phasedVariant.getReference().length() > variant.getReference().length();
            final GATKVariantContextUtils.AlleleMapper alleleMapping;

            if (phasedVariantRefLonger) {
                // not sure what to do
                //missingAllelesReportWriter.print(variant.getContig() + "\t" + variant.getStart() + "\t" + variant.getAlleles() + "\t" + phasedVariant.getAlleles() + "\n");
                //concordanceSummary.mismatchedAlleles(variant);
                //throw new GATKException("phased ref longer than joint ref " + phasedVariant);
                alleleMapping = GATKVariantContextUtils.resolveIncompatibleAlleles(phasedVariant.getReference(), variant, new LinkedHashSet<>());
            } else {

                alleleMapping = GATKVariantContextUtils.resolveIncompatibleAlleles(variant.getReference(), phasedVariant, new LinkedHashSet<>());
            }

//            if (! variant.getAlleles().containsAll(phasedVariant.getAlleles().stream().map(alleleMapping::remap).collect(Collectors.toList()))) {
//                concordanceSummary.mismatchedAlleles(variant);
//                missingAllelesReportWriter.print(variant.getContig() + "\t" + variant.getStart() + "\t" + variant.getAlleles() + "\t" + phasedVariant.getAlleles() + "\n");
//                continue;
//            }

            concordanceSummary.concordantAlleles(variant);
            for (Genotype phasedVariantGenotype : phasedVariant.getGenotypes()) {
                if (!phasedVariantGenotype.isCalled()) {
                    continue;
                }

                if (!variantContainsPhasedAlleles(variant, alleleMapping, phasedVariantGenotype, phasedVariantRefLonger)) {
                    concordanceSummary.mismatchedAlleles(variant);
                    missingAllelesReportWriter.println(variant.getContig() + "\t" +
                            variant.getStart() + "\t" +
                            (phasedVariantRefLonger ? variant.getAlleles().stream().map(alleleMapping::remap).collect(Collectors.toList()) : variant.getAlleles()) + "\t" +
                            phasedVariantGenotype.getSampleName() + "\t" +
                            (phasedVariantRefLonger ? phasedVariant.getAlleles() : phasedVariantGenotype.getAlleles().stream().map(alleleMapping::remap).collect(Collectors.toList())));
                    continue;
                }

                final String sampleName = phasedVariantGenotype.getSampleName();
                final Genotype variantGenotype = genotypesContext.get(sampleName);
                if (!isConcordant(variantGenotype, phasedVariantGenotype, alleleMapping, phasedVariantRefLonger)) {
                    concordanceSummary.discordantGenotype(variant);
                    //logger.warn("non-concordant genotype for sample " + sampleName + " at " + phasedVariant.getStart() + ": " + variantGenotype + " vs " + phasedVariantGenotype);
                    discordantGenotypesReportWriter.println(variant.getContig() +
                            "\t" + variant.getStart() +
                            "\t" + variant.getType() +
                            "\t" + variant.getPhredScaledQual() +
                            "\t" + phasedVariant.getPhredScaledQual() +
                            "\t" + phasedVariant.getAttributeAsString("HAPLOCALLED", "") +
                            "\t" + sampleName +
                            "\t" + variantGenotype.getGenotypeString(false) +
                            "\t" + variantGenotype.getGQ() +
                            "\t" + variantGenotype.getDP() +
                            "\t" + phasedVariantGenotype.getGenotypeString(false) +
                            "\t" + phasedVariantGenotype.getGQ() +
                            "\t" + phasedVariantGenotype.getDP());
                    continue;
                }

                concordanceSummary.concordantGenotype(variant);

                if (! phasedVariantGenotype.isPhased()) {
                    continue;
                }

                GenotypeBuilder genotypeBuilder = new GenotypeBuilder(variantGenotype)
                    .phased(phasedVariantGenotype.isPhased())
                    .alleles(phasedVariantGenotype.getAlleles().stream().map(alleleMapping::remap).collect(Collectors.toList()))
                    .attribute(VCFConstants.PHASE_SET_KEY, phasedVariantGenotype.getAnyAttribute(VCFConstants.PHASE_SET_KEY))
                    .attribute(VCFConstants.PHASE_QUALITY_KEY, phasedVariantGenotype.getAnyAttribute(VCFConstants.PHASE_QUALITY_KEY));

                for (String tag : additionalTags) {
                    genotypeBuilder = genotypeBuilder.attribute(tag, phasedVariantGenotype.getAnyAttribute(tag));
                }
                genotypesContext.replace(genotypeBuilder.make());
            }
        }

        newVariantBuilder = newVariantBuilder.genotypes(genotypesContext);
        vcfWriter.add(newVariantBuilder.make());

    }

    private boolean variantContainsPhasedAlleles(final VariantContext variant, final GATKVariantContextUtils.AlleleMapper alleleMapping, final Genotype phasedVariantGenotype, final boolean phasedVariantRefLonger) {
        if (! phasedVariantRefLonger) {
            return variant.getAlleles().containsAll(phasedVariantGenotype.getAlleles().stream().map(alleleMapping::remap).collect(Collectors.toList()));
        } else {
            return variant.getAlleles().stream().map(alleleMapping::remap).collect(Collectors.toSet()).containsAll(phasedVariantGenotype.getAlleles());
        }
    }

    private boolean isConcordant(final Genotype variantGenotype,
                                 final Genotype phasedVariantGenotype,
                                 final GATKVariantContextUtils.AlleleMapper alleleMapping,
                                 final boolean phasedVariantRefLonger) {
        if (variantGenotype.getPloidy() != phasedVariantGenotype.getPloidy()) {
            return false;
        }

        if (phasedVariantRefLonger) {
            final List<Allele> remappedVariantAlleles = variantGenotype.getAlleles().stream().map(alleleMapping::remap).collect(Collectors.toList());
            for (final Allele phasedVariantAllele : phasedVariantGenotype.getAlleles()) {
                if (! remappedVariantAlleles.contains(phasedVariantAllele)) {
                    return false;
                }

                final Allele variantAllele = remappedVariantAlleles.get(remappedVariantAlleles.indexOf(phasedVariantAllele));
                if (variantGenotype.countAllele(variantAllele) != phasedVariantGenotype.countAllele(phasedVariantAllele)) {
                    return false;
                }

            }
        } else {
            for (final Allele phasedVariantAllele : phasedVariantGenotype.getAlleles()) {
                if (variantGenotype.countAllele(alleleMapping.remap(phasedVariantAllele)) != phasedVariantGenotype.countAllele(phasedVariantAllele)) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public void closeTool(){
        if(vcfWriter != null) {
            vcfWriter.close();
        }

        if (missingAllelesReportWriter != null) {
            missingAllelesReportWriter.close();
        }

        if (discordantGenotypesReportWriter != null) {
            discordantGenotypesReportWriter.close();
        }

        if (concordanceSummaryReportWriter != null) {
            concordanceSummary.write(concordanceSummaryReportWriter);
            concordanceSummaryReportWriter.close();
        }
    }

    static class ConcordanceSummary {
        int variants;
        int snpVariants;
        int indelVariants;

        int matchedVariants;
        int matchedSnpVariants;
        int matchedIndelVariants;

        int genotypeAllelesMatched;
        int genotypeAllelesMismatched;

        int snpGenotypeAllelesMatched;
        int snpGenotypeAllelesMismatched;

        int indelGenotypeAllelesMatched;
        int indelGenotypeAllelesMismatched;

        int genotypesConcordant;
        int genotypesDiscordant;

        int snpGenotypesConcordant;
        int snpGenotypesDiscordant;

        int indelGenotypesConcordant;
        int indelGenotypesDiscordant;

        public void sawVariant(final VariantContext variant) {
            variants++;
            if (variant.isSNP()) {
                snpVariants++;
            }
            if (variant.isIndel() || variant.isMixed()) {
                indelVariants++;
            }
        }

        public void variantSiteMatch(final VariantContext variant) {
            matchedVariants++;
            if (variant.isSNP()) {
                matchedSnpVariants++;
            }
            if (variant.isIndel() || variant.isMixed()) {
                matchedIndelVariants++;
            }

        }

        public void mismatchedAlleles(final VariantContext variant) {
            genotypeAllelesMismatched++;
            if (variant.isSNP()) {
                snpGenotypeAllelesMismatched++;
            }
            if (variant.isIndel()|| variant.isMixed()) {
                indelGenotypeAllelesMismatched++;
            }

        }

        public void concordantAlleles(final VariantContext variant) {
            genotypeAllelesMatched++;
            if (variant.isSNP()) {
                snpGenotypeAllelesMatched++;
            }
            if (variant.isIndel()|| variant.isMixed()) {
                indelGenotypeAllelesMatched++;
            }

        }

        public void discordantGenotype(final VariantContext variant) {
            genotypesDiscordant++;
            if (variant.isSNP()) {
                snpGenotypesDiscordant++;
            }
            if (variant.isIndel()|| variant.isMixed()) {
                indelGenotypesDiscordant++;
            }
        }

        public void concordantGenotype(final VariantContext variant) {
            genotypesConcordant++;
            if (variant.isSNP()) {
                snpGenotypesConcordant++;
            }
            if (variant.isIndel()|| variant.isMixed()) {
                indelGenotypesConcordant++;
            }
        }


        public void write(final PrintWriter concordanceSummaryReportWriter) {
            concordanceSummaryReportWriter.println("TYPE\tTOTAL\tSNP\tINDEL");
            concordanceSummaryReportWriter.println("VARIANTS\t" + variants + "\t" + snpVariants + "\t" + indelVariants);
            concordanceSummaryReportWriter.println("VARIANTS_MATCHED\t" + matchedVariants + "\t" + matchedSnpVariants + "\t" + matchedIndelVariants);
            concordanceSummaryReportWriter.println("GT_ALLELES_MATCHED\t" + genotypeAllelesMatched + "\t" + snpGenotypeAllelesMatched + "\t" + indelGenotypeAllelesMatched);
            concordanceSummaryReportWriter.println("GT_ALLELES_MISMATCHED\t" + genotypeAllelesMismatched + "\t" + snpGenotypeAllelesMismatched + "\t" + indelGenotypeAllelesMismatched);
            concordanceSummaryReportWriter.println("GT_CONCORDANT\t" + genotypesConcordant + "\t" + snpGenotypesConcordant + "\t" + indelGenotypesConcordant);
            concordanceSummaryReportWriter.println("GT_DISCORDANT\t" + genotypesDiscordant + "\t" + snpGenotypesDiscordant + "\t" + indelGenotypesDiscordant);
        }

    }

}
