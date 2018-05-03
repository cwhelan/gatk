package org.broadinstitute.hellbender.tools.spark.linkedreads;

import htsjdk.samtools.util.BufferedLineReader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.LinkedReadsProgramGroup;
import org.broadinstitute.hellbender.engine.datasources.ReferenceMultiSource;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.spark.sv.evidence.ReadMetadata;
import org.broadinstitute.hellbender.tools.spark.sv.utils.*;
import org.broadinstitute.hellbender.tools.spark.utils.IntHistogram;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import scala.Tuple2;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.broadinstitute.hellbender.tools.spark.linkedreads.ExtractLinkedReadsSpark.ReadInfo;

@CommandLineProgramProperties(
        summary = "Finds linked read evidence links",
        oneLineSummary = "FindLinkedReadEvidenceLinks on Spark",
        programGroup = LinkedReadsProgramGroup.class
)
public class FindLinkedReadEvidenceLinks extends GATKSparkTool {
    private static final long serialVersionUID = 1L;
    public static final int MAX_TRACKED_VALUE = 20000;

    protected final Logger logger = LogManager.getLogger(this.getClass());

    @Argument(doc = "uri for the output file",
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME, fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
            optional = true)
    public String out;

    @Argument(doc = "input linked read file", shortName = "input-linked-reads", fullName = "input-linked-reads")
    public File inputLinkedReads = null;

    @Argument(doc = "barcode whitelist", shortName = "barcode-whitelist", fullName = "barcode-whitelist")
    public File barcodeWhitelist = null;

    @Argument(doc = "expected gap length percentile", shortName = "gap-percentile", fullName = "gap-percentile")
    public float gapPercentile = 0.5f;

    @Override
    public boolean requiresReference() {
        return true;
    }

    @Override
    protected void runTool(final JavaSparkContext ctx) {

        logger.info("Loading linked reads");
        final ReferenceMultiSource reference = getReference();

        final Map<String, Integer> contigNameToIdMap = ReadMetadata.buildContigNameToIDMap(getReferenceSequenceDictionary());
        final String[] contigNames = ReadMetadata.buildIDToNameArray(contigNameToIdMap);

        final Map<String, Integer> barcodeToIdMap = buildBarcodeToIDMap(barcodeWhitelist);
        final Broadcast<Map<String, Integer>> broadcastBarcodeIdMap = ctx.broadcast(barcodeToIdMap);
        final String[] barcodeNames = ReadMetadata.buildIDToNameArray(barcodeToIdMap);

        final Broadcast<Map<String, Integer>> broadcastContigNameMap = ctx.broadcast(contigNameToIdMap);

        final JavaPairRDD<Integer, Tuple2<SVInterval, List<ReadInfo>>> barcodeIntervalsWithReads;
        barcodeIntervalsWithReads = parseBarcodeIntervals(ctx, broadcastContigNameMap, broadcastBarcodeIdMap, inputLinkedReads).cache();
        logger.info("Done loading linked reads");

        final IntHistogram fullIntHistogram = getGapSizeHistogram(barcodeIntervalsWithReads);

        logger.info("Median gap size: " + fullIntHistogram.getCDF().median());
        logger.info("90th: " + fullIntHistogram.getCDF().popStat(0.90f));
        logger.info("95th: " + fullIntHistogram.getCDF().popStat(0.95f));
        logger.info("99th: " + fullIntHistogram.getCDF().popStat(0.99f));

        final int expectedGapLength = fullIntHistogram.getCDF().popStat(gapPercentile);

        final JavaPairRDD<Integer, SVInterval> barcodeIntervalsWithoutReads = barcodeIntervalsWithReads.mapValues(Tuple2::_1);
        barcodeIntervalsWithReads.unpersist();

//        JavaPairRDD<Integer, Tuple2<SVInterval, Long>> barcodesWithoutReadsWithIds =
//                barcodeIntervalsWithoutReads.zipWithUniqueId()
//                        .mapToPair(pair -> new Tuple2<>(pair._1._1, new Tuple2<>(pair._1._2(), pair._2)));

        final Map<Integer, SVIntervalTree<Boolean>> barcodeEndTrees = getIntervalEndsTrees(expectedGapLength, barcodeIntervalsWithoutReads);

            try (final OutputStreamWriter writer =
                         new OutputStreamWriter(new BufferedOutputStream(BucketUtils.createFile("bar")))) {
                barcodeEndTrees.keySet().iterator().forEachRemaining(entry -> {
                    final SVIntervalTree<Boolean> entries = barcodeEndTrees.get(entry);

                        entries.forEach(e -> {
                            try {
                                writer.write( contigNames[e.getInterval().getContig()] + "\t" + e.getInterval().getStart() + "\t" + e.getInterval().getEnd() + "\t" + barcodeNames[entry] + "\t-1\t" + (e.getValue() ? "+" : "-") + "\n");
                            } catch (IOException e1) {
                                e1.printStackTrace();
                            }
                        });
                });
            } catch ( final IOException ioe ) {
                throw new GATKException("Can't write target links to "+ "bar", ioe);
            }


        final Broadcast<Map<Integer, SVIntervalTree<Boolean>>> broadcastIntervalEnds = ctx.broadcast(barcodeEndTrees);

        final JavaRDD<Tuple2<PairedStrandedIntervals, Tuple2<Set<Integer>, Tuple2<Integer, Integer>>>> linksWithEnoughOverlappers = barcodeIntervalsWithoutReads
                .mapToPair(pair -> new Tuple2<>(pair._2(), pair._1))
                .repartitionAndSortWithinPartitions(
                        new ByContigPartitioner(broadcastContigNameMap.getValue().size())
                )
                .mapPartitions((iter) -> findLinksWithEnoughOverlappers(expectedGapLength, iter, broadcastIntervalEnds.getValue()));

        final List<Tuple2<PairedStrandedIntervals, Tuple2<Set<Integer>, Tuple2<Integer, Integer>>>> collectedLinks = linksWithEnoughOverlappers.collect();
        writeEvidenceLinks(collectedLinks, out, contigNames, barcodeNames);
    }

    protected static Iterator<Tuple2<PairedStrandedIntervals, Tuple2<Set<Integer>, Tuple2<Integer, Integer>>>> findLinksWithEnoughOverlappers(final int expectedGapLength,
                                                                                                                                     final Iterator<Tuple2<SVInterval, Integer>> moleculeIterator,
                                                                                                                                     final Map<Integer, SVIntervalTree<Boolean>> intervalEnds) {
        final PairedStrandedIntervalTree<Tuple2<Set<Integer>, Tuple2<Integer, Integer>>> results = new PairedStrandedIntervalTree<>();
        final PairedStrandedIntervalTree<Integer> unclusteredLinks = new PairedStrandedIntervalTree<>();
        while (moleculeIterator.hasNext()) {
            final Tuple2<SVInterval, Integer> nextMolecule = moleculeIterator.next();
            final Integer moleculeBarcode = nextMolecule._2;
            final SVInterval moleculeInterval = nextMolecule._1();
            final SVInterval moleculeLeftEndInterval = new SVInterval(
                    moleculeInterval.getContig(),
                    Math.max(0, moleculeInterval.getStart() - expectedGapLength),
                    moleculeInterval.getStart());
            final SVInterval moleculeRightEndInterval = new SVInterval(
                    moleculeInterval.getContig(),
                    moleculeInterval.getEnd(),
                    moleculeInterval.getEnd() + expectedGapLength);

            if (moleculeLeftEndInterval.getStart() == 158919072) {
                System.err.println("foo");
            }

            addClusteredLinksToResults(results, unclusteredLinks, moleculeLeftEndInterval);

            final Iterator<Tuple2<PairedStrandedIntervals, Integer>> cleanupIterator = unclusteredLinks.iterator();
            while (cleanupIterator.hasNext()) {
                final Tuple2<PairedStrandedIntervals, Integer> link = cleanupIterator.next();

                if (! link._1().getLeft().getInterval().isDisjointFrom(moleculeLeftEndInterval)) {
                    break;
                }

                final Iterator<Tuple2<PairedStrandedIntervals, Integer>> overlapIterator = unclusteredLinks.overlappers(link._1());
                while (overlapIterator.hasNext()) {
                    final Tuple2<PairedStrandedIntervals, Integer> overlapper = overlapIterator.next();
                    if (!overlapper._1().getLeft().getInterval().isDisjointFrom(moleculeLeftEndInterval)) {
                        break;
                    }
                }

                cleanupIterator.remove();
            }

            final int moleculeStart = moleculeInterval.getStart();
            final int moleculeEnd = moleculeInterval.getEnd();
            intervalEnds.get(moleculeBarcode).forEach(entry -> {
                final SVInterval endInterval = entry.getInterval();

                if (moleculeStart != (entry.getInterval().getEnd()) &&
                        moleculeEnd != (entry.getInterval().getStart())) {
                    if (moleculeLeftEndInterval.isUpstreamOf(endInterval)) {
                        final PairedStrandedIntervals leftEndPair = new PairedStrandedIntervals(
                                new StrandedInterval(moleculeLeftEndInterval, false),
                                new StrandedInterval(endInterval, entry.getValue()));
                        System.err.println(toBedpeString(leftEndPair));
                        unclusteredLinks.put(leftEndPair, moleculeBarcode);
                    }
                    if (moleculeRightEndInterval.isUpstreamOf(endInterval)) {
                        final PairedStrandedIntervals rightEndPair = new PairedStrandedIntervals(
                                new StrandedInterval(moleculeRightEndInterval, true),
                                new StrandedInterval(endInterval, entry.getValue()));
                        unclusteredLinks.put(rightEndPair, moleculeBarcode);
                        System.err.println(toBedpeString(rightEndPair));
                    }
                }

            });

        }
        addClusteredLinksToResults(results, unclusteredLinks, null);
        return results.iterator();
    }

    private static void addClusteredLinksToResults(final PairedStrandedIntervalTree<Tuple2<Set<Integer>, Tuple2<Integer, Integer>>> results,
                                                   final PairedStrandedIntervalTree<Integer> unclusteredLinks,
                                                   final SVInterval moleculeLeftEndInterval) {
        final Iterator<Tuple2<PairedStrandedIntervals, Integer>> overlapScanIterator = unclusteredLinks.iterator();
        while (overlapScanIterator.hasNext()) {
            final Tuple2<PairedStrandedIntervals, Integer> matchingLink = overlapScanIterator.next();
            final Integer linkBarcode = matchingLink._2();

            if (moleculeLeftEndInterval != null && !matchingLink._1().getLeft().getInterval().isDisjointFrom(moleculeLeftEndInterval)) {
                break;
            }

            Set<Integer> overlapperBarcodes = new HashSet<>();
            overlapperBarcodes.add(linkBarcode);
            int overlappers = 0;
            final Iterator<Tuple2<PairedStrandedIntervals, Integer>> overlapIterator = unclusteredLinks.overlappers(matchingLink._1());
            while (overlapIterator.hasNext()) {
                final Tuple2<PairedStrandedIntervals, Integer> overlappingLink = overlapIterator.next();
                if (overlappingLink._1().equals(matchingLink._1) && overlappingLink._2().equals(matchingLink._2())) {
                    continue;
                }
                if (moleculeLeftEndInterval != null && ! overlappingLink._1().getLeft().getInterval().isDisjointFrom(moleculeLeftEndInterval)) {
                    break;
                }
                final Integer overlapperBarcode = overlappingLink._2();
                if (!overlapperBarcode.equals(linkBarcode)) {
                    overlapperBarcodes.add(overlapperBarcode);
                    overlappers++;
                }
            }
            if (overlappers > 0) {
                final PairedStrandedIntervals linkToAdd = matchingLink._1;
                PairedStrandedIntervals newLink = new PairedStrandedIntervals(
                        linkToAdd.getLeft(),
                        linkToAdd.getRight());
                final Set<Integer> newOverlapperBarcodes = new HashSet<>(overlapperBarcodes);
                final boolean leftStrand = matchingLink._1.getLeft().getStrand();
                int leftEventCutoff = leftStrand ? matchingLink._1().getLeft().getInterval().getStart() : matchingLink._1().getLeft().getInterval().getEnd();
                final boolean rightStrand = matchingLink._1.getRight().getStrand();
                int rightEventCutoff = rightStrand ? matchingLink._1().getRight().getInterval().getStart() : matchingLink._1().getRight().getInterval().getEnd();

                boolean maybeMoreOverlappers = true;
                while(maybeMoreOverlappers) {
                    final Iterator<Tuple2<PairedStrandedIntervals, Tuple2<Set<Integer>, Tuple2<Integer, Integer>>>> previousOverlappers =
                            results.overlappers(newLink);
                    if (!previousOverlappers.hasNext()) {
                        maybeMoreOverlappers = false;
                    } else {
                        // todo: maybe slop here?
                        final Tuple2<PairedStrandedIntervals, Tuple2<Set<Integer>, Tuple2<Integer, Integer>>> previousOverlapper = previousOverlappers.next();
                        newLink = new PairedStrandedIntervals(
                                new StrandedInterval(newLink.getLeft().getInterval().join(
                                        previousOverlapper._1().getLeft().getInterval()
                                ), newLink.getLeft().getStrand()),
                                new StrandedInterval(newLink.getRight().getInterval().join(
                                        previousOverlapper._1().getRight().getInterval()
                                ), newLink.getRight().getStrand()));
                        newOverlapperBarcodes.addAll(previousOverlapper._2()._1());
                        leftEventCutoff = leftStrand ? Math.max(leftEventCutoff, previousOverlapper._2()._2()._1()) : Math.min(leftEventCutoff, previousOverlapper._2()._2()._1());
                        rightEventCutoff = rightStrand ? Math.max(rightEventCutoff, previousOverlapper._2()._2()._2()) : Math.min(rightEventCutoff, previousOverlapper._2()._2()._2());
                        previousOverlappers.remove();
                    }
                }
                results.put(newLink, new Tuple2<>(newOverlapperBarcodes, new Tuple2<>(leftEventCutoff, rightEventCutoff)));
            }
        }
    }

    private static void writeEvidenceLinks(final List<Tuple2<PairedStrandedIntervals, Tuple2<Set<Integer>, Tuple2<Integer, Integer>>>> targetLinks,
                                           final String targetLinkFile,
                                           final String[] contigNames,
                                           final String[] barcodeNames) {
        if ( targetLinkFile != null ) {
            try (final OutputStreamWriter writer =
                         new OutputStreamWriter(new BufferedOutputStream(BucketUtils.createFile(targetLinkFile)))) {
                targetLinks.iterator().forEachRemaining(entry -> {
                    final String bedpeRecord = toBedpeString(entry, contigNames, barcodeNames);
                    try {
                        writer.write(bedpeRecord + "\n");
                    } catch (final IOException ioe) {
                        throw new GATKException("Can't write target links to "+ targetLinkFile, ioe);
                    }
                });
            } catch ( final IOException ioe ) {
                throw new GATKException("Can't write target links to "+ targetLinkFile, ioe);
            }
        }
    }

    private static String toBedpeString(final PairedStrandedIntervals pair) {
        final SVInterval sourceInterval = pair.getLeft().getInterval();
        final SVInterval targetInterval = pair.getRight().getInterval();

        final boolean leftStrand = pair.getLeft().getStrand();
        final int leftStart = sourceInterval.getStart() - 1;
        final int leftEnd = sourceInterval.getEnd();

        final boolean rightStrand = pair.getRight().getStrand();
        final int rightStart = targetInterval.getStart() - 1;
        final int rightEnd = targetInterval.getEnd();

        return sourceInterval.getContig() + "\t" + leftStart + "\t" + leftEnd +
                "\t" + targetInterval.getContig() + "\t" + rightStart + "\t" + rightEnd +
                "\t"  + sourceInterval.getContig() + "_" + leftStart + "_" + leftEnd + "_" +
                targetInterval.getContig() + "_" + rightStart + "_" + rightEnd + "_" +
                (leftStrand ? "P" : "M") + "_" + (rightStrand ? "P" : "M") + "\t1" +
                "\t" + (leftStrand ? "+" : "-") + "\t" + (rightStrand ? "+" : "-");
    }

    public static String toBedpeString(Tuple2<PairedStrandedIntervals, Tuple2<Set<Integer>, Tuple2<Integer, Integer>>> entry, final String[] contigNames, final String[] barcodeNames) {
        final SVInterval sourceInterval = entry._1().getLeft().getInterval();
        final SVInterval targetInterval = entry._1().getRight().getInterval();
        String sourceContigName = contigNames[sourceInterval.getContig()];
        String targetContigName = contigNames[targetInterval.getContig()];

        final int leftEventCut = entry._2()._2()._1();
        final int rightEventCut = entry._2()._2()._2();

        final boolean leftStrand = entry._1().getLeft().getStrand();
        final int leftStart = leftStrand ? leftEventCut : sourceInterval.getStart() - 1;
        final int leftEnd = leftStrand ? sourceInterval.getEnd() : leftEventCut;

        final boolean rightStrand = entry._1().getRight().getStrand();
        final int rightStart = rightStrand ? rightEventCut : targetInterval.getStart() - 1;
        final int rightEnd = rightStrand ? targetInterval.getEnd() : rightEventCut;

        final Set<Integer> barcodes = entry._2()._1();

        return sourceContigName + "\t" + leftStart + "\t" + leftEnd +
                "\t" + targetContigName + "\t" + rightStart + "\t" + rightEnd +
                "\t"  + sourceContigName + "_" + leftStart + "_" + leftEnd + "_" +
                targetContigName + "_" + rightStart + "_" + rightEnd + "_" +
                (leftStrand ? "P" : "M") + "_" + (rightStrand ? "P" : "M") + "_" + barcodes.size() +
                "\t" + barcodes.size() + "\t" + (leftStrand ? "+" : "-") + "\t" + (rightStrand ? "+" : "-")
                + "\t" + Utils.join(",", barcodes.stream().map(i -> barcodeNames[i]).collect(Collectors.toList()));
    }

    private Map<String, Integer> buildBarcodeToIDMap(final File barcodeWhitelist) {
        final Map<String, Integer> barcodeMap = new HashMap<>(4000000);
        try(FileInputStream inputStream = new FileInputStream(barcodeWhitelist.getPath())) {
            final BufferedLineReader reader = new BufferedLineReader(inputStream);
            String line = reader.readLine();
            int index = 0;
            while ( line != null ) {
                barcodeMap.put(line + "-1", index);
                line = reader.readLine();
                index = index + 1;
            }

        } catch (IOException e) {
            throw new GATKException("unable to read barcode whitelist file", e);
        }
        return barcodeMap;
    }

    private Map<Integer, SVIntervalTree<Boolean>> getIntervalEndsTrees(final int expectedGapLength,
                                                                       final JavaPairRDD<Integer, SVInterval> barcodesWithoutReadsWithIds) {
        final JavaPairRDD<Integer, SVIntervalTree<Boolean>> barcodeTrees = barcodesWithoutReadsWithIds.aggregateByKey(
                null,
                (tree, interval) -> {
                    if (tree == null) {
                        tree = new SVIntervalTree<>();
                    }
                    final SVInterval leftInterval = new SVInterval(
                            interval.getContig(),
                            Math.max(0, interval.getStart() - expectedGapLength),
                            interval.getStart());
                    tree.put(leftInterval, false);
                    final SVInterval rightInterval = new SVInterval(
                            interval.getContig(),
                            interval.getEnd(),
                            interval.getEnd() + expectedGapLength);
                    tree.put(rightInterval, true);

                    return tree;
                },
                (tree1, tree2) -> {

                    if (tree1 == null) {
                        tree1 = new SVIntervalTree<>();
                    }
                    if (tree2 == null) {
                        tree2 = new SVIntervalTree<>();
                    }
                    final SVIntervalTree<Boolean> combinedTree = new SVIntervalTree<>();
                    tree1.forEach(e -> combinedTree.put(e.getInterval(), e.getValue()));
                    tree2.forEach(e -> combinedTree.put(e.getInterval(), e.getValue()));
                    return combinedTree;
                }
        );

        final JavaPairRDD<Integer, SVIntervalTree<Boolean>> filteredTrees =
                barcodeTrees.filter(pair -> pair._2().size() > 1);

        return filteredTrees.collectAsMap();
    }

    private IntHistogram getGapSizeHistogram(final JavaPairRDD<Integer, Tuple2<SVInterval, List<ReadInfo>>> barcodeIntervalsWithReads) {
        final List<IntHistogram> partitionHistograms = barcodeIntervalsWithReads.mapPartitions(iter -> {
            IntHistogram intHistogram = new IntHistogram(MAX_TRACKED_VALUE);
            while (iter.hasNext()) {
                final Tuple2<Integer, Tuple2<SVInterval, List<ReadInfo>>> next = iter.next();
                final List<ReadInfo> readInfos = next._2()._2();
                for (int i = 1; i < readInfos.size(); i++) {
                    final int gap = readInfos.get(i).getStart() - readInfos.get(i - 1).getStart();
                    intHistogram.addObservation(gap);
                }
            }
            return SVUtils.singletonIterator(intHistogram);
        }).collect();


        return partitionHistograms.stream().reduce(
                new IntHistogram(MAX_TRACKED_VALUE),
                (h1, h2) -> {
                    h1.addObservations(h2);
                    return h1;
                });
    }

    private JavaPairRDD<Integer, Tuple2<SVInterval, List<ReadInfo>>> parseBarcodeIntervals(final JavaSparkContext ctx,
                                                                                          final Broadcast<Map<String, Integer>> broadcastContigNameMap,
                                                                                          final Broadcast<Map<String, Integer>> broadcastBarcodeIdMap, final File inputLinkedReads) {
        final JavaRDD<String> stringJavaRDD = ctx.textFile(inputLinkedReads.getAbsolutePath());
        logger.info("loaded text file into " + stringJavaRDD.getNumPartitions() + " partitions");
        JavaRDD<String> repartitionedFileData = stringJavaRDD.repartition(ctx.defaultParallelism());
        final JavaPairRDD<Integer, Tuple2<SVInterval, List<ReadInfo>>> moleculeIntervals =
                repartitionedFileData.mapToPair(line -> parseBarcodeIntervalLine(line, broadcastContigNameMap.getValue(), broadcastBarcodeIdMap.getValue()));
        logger.info("molecule intervals are in " + repartitionedFileData.getNumPartitions() + " partitions");
        return moleculeIntervals;

    }

    private Tuple2<Integer, Tuple2<SVInterval, List<ReadInfo>>> parseBarcodeIntervalLine(final String line, final Map<String, Integer> contigNameMap, final Map<String, Integer> barcodeIdMap) {
        final String[] fields = line.split("\t");
        int contigID = contigNameMap.get(fields[0]);
        int moleculeStart = Integer.parseInt(fields[1]);
        final SVInterval interval = new SVInterval(contigID, moleculeStart, Integer.parseInt(fields[2]));
        final Integer barcode = barcodeIdMap.get(fields[3]);
        if (barcode == null) {
            throw new GATKException("Barcode not on whitelist: " + fields[3]);
        }
        final int numReads = Integer.valueOf(fields[9]);
        final List<Integer> sizes = Arrays.stream(fields[10].split(",")).map(Integer::valueOf).collect(Collectors.toList());
        final List<Integer> starts = Arrays.stream(fields[11].split(",")).map(Integer::valueOf).collect(Collectors.toList());
        final List<ReadInfo> readInfos = new ArrayList<>(numReads);
        for (int i = 0; i < numReads; i++) {
            final ReadInfo readInfo = new ReadInfo(contigID,
                    moleculeStart + starts.get(i),
                    moleculeStart + starts.get(i) + sizes.get(i), true, -1);
            readInfos.add(readInfo);
        }

        return new Tuple2<>(barcode, new Tuple2<>(interval, readInfos));
    }

    private static class ByContigPartitioner extends Partitioner {
        private static final long serialVersionUID = 1L;
        private final int numContigs;

        public ByContigPartitioner(final int numContigs) {
            this.numContigs = numContigs;
        }

        @Override
        public int getPartition(final Object key) {
            final SVInterval interval = (SVInterval) key;
            return interval.getContig();
        }

        @Override
        public int numPartitions() {
            return numContigs;
        }
    }
}