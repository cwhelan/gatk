package org.broadinstitute.hellbender.tools.spark.linkedreads;

import htsjdk.tribble.Feature;
import htsjdk.tribble.bed.BEDFeature;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.LinkedReadsProgramGroup;
import org.broadinstitute.hellbender.engine.*;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.spark.sv.evidence.ReadMetadata;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVInterval;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVIntervalTree;
import org.broadinstitute.hellbender.utils.SimpleInterval;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.*;

@CommandLineProgramProperties(
        summary = "Find intervals with max barcode overlap to the barcodes found in the target intervals",
        oneLineSummary = "Find intervals with max barcode overlap to the barcodes found in the target intervals",
        programGroup = LinkedReadsProgramGroup.class,
        omitFromCommandLine = true
)
public class PlaceTargetIntervals extends FeatureWalker<BEDFeature> {

    @Argument(doc = "uri for the output file",
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME, fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
            optional = true)
    public String out;

    @Argument(fullName = "barcode-intervals", doc = "Barcode intervals file created by ExtractLinkedReadsSpark")
    public File linkedReadsFile;

    @Argument(fullName = "target-intervals", doc = "Intervals to be placed")
    public File targetIntervals;
    private Map<SimpleInterval, Set<String>> barcodesForTargets;
    private SVIntervalTree<String> targetIntervalTree = new SVIntervalTree<>();
    private Map<String, Integer> contigNameToIdMap;
    private String currentContig;
    private Map<String, Set<SimpleInterval>> targetsByBarcode;
    private PriorityQueue<BEDFeature> endingMolecules;
    private Map<SimpleInterval, Set<String>> barcodeOverlappers = new HashMap<>();
    private OutputStreamWriter writer;
    private Map<SimpleInterval, Integer> peaksByTarget = new HashMap<>();
    private Map<SimpleInterval, Set<String>> peakBarcodesByTarget = new HashMap<>();

    @Override
    protected boolean isAcceptableFeatureType(final Class<? extends Feature> featureType) {
        return featureType.isAssignableFrom(BEDFeature.class);
    }

    @Override
    public boolean requiresReference() {
        return true;
    }

    @Override
    public File getDrivingFeatureFile() {
        return linkedReadsFile;
    }

    @Override
    public void onTraversalStart() {
        super.onTraversalStart();

        barcodesForTargets = new HashMap<>();
        targetsByBarcode = new HashMap<>();
        contigNameToIdMap = ReadMetadata.buildContigNameToIDMap(getBestAvailableSequenceDictionary());
        currentContig = null;

        try ( final FeatureDataSource<BEDFeature> linkedReadsDataSource = new FeatureDataSource<>(linkedReadsFile) ) {
            try (final FeatureDataSource<BEDFeature> targetIntervalDataSource = new FeatureDataSource<>(targetIntervals)) {

                for (final BEDFeature targetInterval : targetIntervalDataSource) {
                    final SimpleInterval targetSimpleInterval = new SimpleInterval(targetInterval.getContig(), targetInterval.getStart(), targetInterval.getEnd());
                    final SVInterval targetSVInterval = new SVInterval(contigNameToIdMap.get(targetSimpleInterval.getContig()), targetSimpleInterval.getStart(), targetSimpleInterval.getEnd());
                    targetIntervalTree.put(targetSVInterval, targetInterval.getName());
                    final Set<String> barcodesInTargetInterval = new HashSet<>();

                    final Iterator<BEDFeature> linkedReadsInTargetIterator
                            = linkedReadsDataSource.query(targetSimpleInterval);
                    for (Iterator<BEDFeature> it = linkedReadsInTargetIterator; it.hasNext(); ) {
                        final BEDFeature linkedRead = it.next();
                        barcodesInTargetInterval.add(linkedRead.getName());
                    }
                    if (! barcodesInTargetInterval.isEmpty()) {
                        barcodesForTargets.put(targetSimpleInterval, barcodesInTargetInterval);
                    }
                    barcodesInTargetInterval.forEach(bc -> addBarcodeTargetMapping(bc, targetSimpleInterval));
                    System.err.println("tracking " + barcodesInTargetInterval.size() + " barcodes for target interval " + targetSimpleInterval);
                }
            }
        }


        endingMolecules = new PriorityQueue<>(Comparator.comparing(BEDFeature::getEnd));

        writer = new OutputStreamWriter(new BufferedOutputStream(BucketUtils.createFile(out)));

    }

    private void addBarcodeTargetMapping(final String bc, final SimpleInterval targetSimpleInterval) {
        if (!targetsByBarcode.containsKey(bc)) {
            targetsByBarcode.put(bc, new HashSet<>());
        }
        targetsByBarcode.get(bc).add(targetSimpleInterval);
    }

    private void resetCounts() {
        for (final SimpleInterval target : barcodesForTargets.keySet()) {
            barcodeOverlappers.put(target, new HashSet<>());
        }
    }

    @Override
    public void apply(final BEDFeature linkedRead, final ReadsContext readsContext, final ReferenceContext referenceContext, final FeatureContext featureContext) {
        final String contig = linkedRead.getContig();
        final String newBarcode = linkedRead.getName();
        if (newBarcode.equals("GATTCAGGTAGCTGCC-1")) {
            System.err.println("found GATTCAGGTAGCTGCC-1");
        }
        if (! targetsByBarcode.containsKey(newBarcode)) {
            return;
        }

        final SVInterval linkedReadInterval = new SVInterval(contigNameToIdMap.get(contig), linkedRead.getStart(), linkedRead.getEnd());


        if (targetIntervalTree.hasOverlapper(linkedReadInterval)) {
            return;
        }


        if (currentContig == null || !currentContig.equals(contig)) {
            if (currentContig != null) {
                drainEndQueue(currentContig, getBestAvailableSequenceDictionary().getSequence(currentContig).getSequenceLength());
            }
            resetCounts();

        }

        currentContig = contig;

        final int newStart = linkedRead.getStart();
        drainEndQueue(contig, newStart);

        final Set<SimpleInterval> linkedTargets = targetsByBarcode.get(newBarcode);
        for (final SimpleInterval target : linkedTargets) {
            barcodeOverlappers.get(target).add(newBarcode);

            if (!peakBarcodesByTarget.containsKey(target) || barcodeOverlappers.get(target).size() > peakBarcodesByTarget.get(target).size()) {
                peakBarcodesByTarget.put(target, new HashSet<>(barcodeOverlappers.get(target)));
                peaksByTarget.put(target, linkedRead.getStart());
            }

        }
        endingMolecules.add(linkedRead);

    }

    private void drainEndQueue(final String contig, final int newStart) {
        while(endingMolecules.size() > 0 && endingMolecules.peek().getEnd() <= newStart) {
            final BEDFeature endingMolecule = endingMolecules.poll();
            final String endingBarcode = endingMolecule.getName();
            if (endingBarcode.equals("GATTCAGGTAGCTGCC-1")) {
                System.err.println("ending GATTCAGGTAGCTGCC-1");
            }

            final Set<SimpleInterval> linkedTargets = targetsByBarcode.get(endingBarcode);
            for (final SimpleInterval target : linkedTargets) {
                if (barcodeOverlappers.get(target).remove(endingBarcode)) {

                    if (barcodeOverlappers.get(target).size() == 0) {
                        try {
                            final Set<String> peakBarcodes = peakBarcodesByTarget.get(target);
                            if (peakBarcodes == null) {
                                throw new GATKException("shouldn't be here");
                            }
                            writer.write(target.toString() + "\t" + contig + "\t" + peaksByTarget.get(target) + "\t" + peakBarcodes.size() + "\t" + String.join(",", peakBarcodes) + "\n");
                        } catch (IOException e) {
                            throw new GATKException("Could not write to " + out, e);
                        }
                        peaksByTarget.remove(target);
                        peakBarcodesByTarget.remove(target);
                    }
                } else {
                    throw new GATKException("what am i doing here?");
                }

            }
        }
    }

    @Override
    public Object onTraversalSuccess() {
        drainEndQueue(currentContig, getBestAvailableSequenceDictionary().getSequence(currentContig).getSequenceLength());
        return null;
    }

    @Override
    public void closeTool() {
        super.closeTool();
        try {
            writer.close();
        } catch (IOException e) {
            throw new GATKException("could not close file " + out, e);
        }
    }
}
