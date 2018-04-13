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

        barcodeIntervalsWithReads.unpersist();
        final JavaPairRDD<Integer, SVInterval> barcodeIntervalsWithoutReads = barcodeIntervalsWithReads.mapValues(Tuple2::_1);

//        JavaPairRDD<Integer, Tuple2<SVInterval, Long>> barcodesWithoutReadsWithIds =
//                barcodeIntervalsWithoutReads.zipWithUniqueId()
//                        .mapToPair(pair -> new Tuple2<>(pair._1._1, new Tuple2<>(pair._1._2(), pair._2)));

        final Map<Integer, SVIntervalTree<Boolean>> barcodeEndTrees = getIntervalEndsTrees(expectedGapLength, barcodeIntervalsWithoutReads);
        final Broadcast<Map<Integer, SVIntervalTree<Boolean>>> broadcastIntervalEnds = ctx.broadcast(barcodeEndTrees);

        final JavaRDD<Tuple2<PairedStrandedIntervals, Set<Integer>>> linksWithEnoughOverlappers = barcodeIntervalsWithoutReads
                .mapToPair(pair -> new Tuple2<>(pair._2(), pair._1))
                .repartitionAndSortWithinPartitions(
                        new ByContigPartitioner(broadcastContigNameMap.getValue().size())
                )
                .mapPartitions((iter) -> {
                    final PairedStrandedIntervalTree<Set<Integer>> results = new PairedStrandedIntervalTree<>();
                    final PairedStrandedIntervalTree<Integer> links = new PairedStrandedIntervalTree<>();
                    final Map<Integer, SVIntervalTree<Boolean>> intervalEnds = broadcastIntervalEnds.getValue();
                    while (iter.hasNext()) {
                        final Tuple2<SVInterval, Integer> next = iter.next();
                        final Integer barcode = next._2;
                        final SVInterval moleculeInterval = next._1();
                        final SVInterval leftInterval = new SVInterval(
                                moleculeInterval.getContig(),
                                Math.max(0, moleculeInterval.getStart() - expectedGapLength),
                                moleculeInterval.getStart());
                        final SVInterval rightInterval = new SVInterval(
                                moleculeInterval.getContig(),
                                moleculeInterval.getEnd(),
                                moleculeInterval.getEnd() + expectedGapLength);

                        final Iterator<Tuple2<PairedStrandedIntervals, Integer>> overlapScanIterator = links.iterator();
                        while (overlapScanIterator.hasNext()) {
                            final Tuple2<PairedStrandedIntervals, Integer> link = overlapScanIterator.next();
                            final Integer linkBarcode = link._2();

                            if (!link._1().getLeft().getInterval().isDisjointFrom(leftInterval)) {
                                break;
                            }

                            Set<Integer> overlapperBarcodes = new HashSet<>();
                            overlapperBarcodes.add(linkBarcode);
                            int overlappers = 0;
                            final Iterator<Tuple2<PairedStrandedIntervals, Integer>> overlapIterator = links.overlappers(link._1());
                            while (overlapIterator.hasNext()) {
                                final Tuple2<PairedStrandedIntervals, Integer> overlapper = overlapIterator.next();
                                if (! overlapper._1().getLeft().getInterval().isDisjointFrom(leftInterval)) {
                                    break;
                                }
                                final Integer overlapperBarcode = overlapper._2();
                                if (!overlapperBarcode.equals(linkBarcode)) {
                                    overlapperBarcodes.add(overlapperBarcode);
                                    overlappers++;
                                }
                            }
                            if (overlappers > 3) {
                                final PairedStrandedIntervals newGoodPairedStrandedIntervals = link._1;
                                PairedStrandedIntervals newLink = new PairedStrandedIntervals(
                                        newGoodPairedStrandedIntervals.getLeft(),
                                        newGoodPairedStrandedIntervals.getRight());
                                final Set<Integer> newOverlapperBarcodes = new HashSet<>(overlapperBarcodes);

                                boolean maybeMoreOverlappers = true;
                                while(maybeMoreOverlappers) {
                                    final Iterator<Tuple2<PairedStrandedIntervals, Set<Integer>>> previousOverlappers =
                                            results.overlappers(newLink);
                                    if (!previousOverlappers.hasNext()) {
                                        maybeMoreOverlappers = false;
                                    } else {
                                        // todo: maybe slop here?
                                        final Tuple2<PairedStrandedIntervals, Set<Integer>> previousOverlapper = previousOverlappers.next();
                                        newLink = new PairedStrandedIntervals(
                                                new StrandedInterval(newLink.getLeft().getInterval().join(
                                                        previousOverlapper._1().getLeft().getInterval()
                                                ), newLink.getLeft().getStrand()),
                                                new StrandedInterval(newLink.getRight().getInterval().join(
                                                        previousOverlapper._1().getRight().getInterval()
                                                ), newLink.getRight().getStrand()));
                                        newOverlapperBarcodes.addAll(previousOverlapper._2());
                                        previousOverlappers.remove();
                                    }
                                }
                                results.put(newGoodPairedStrandedIntervals, newOverlapperBarcodes);
                            }
                        }

                        final Iterator<Tuple2<PairedStrandedIntervals, Integer>> cleanupIterator = links.iterator();
                        while (cleanupIterator.hasNext()) {
                            final Tuple2<PairedStrandedIntervals, Integer> link = cleanupIterator.next();

                            if (! link._1().getLeft().getInterval().isDisjointFrom(leftInterval)) {
                                break;
                            }

                            final Iterator<Tuple2<PairedStrandedIntervals, Integer>> overlapIterator = links.overlappers(link._1());
                            while (overlapIterator.hasNext()) {
                                final Tuple2<PairedStrandedIntervals, Integer> overlapper = overlapIterator.next();
                                if (!overlapper._1().getLeft().getInterval().isDisjointFrom(leftInterval)) {
                                    break;
                                }
                            }

                            cleanupIterator.remove();
                        }

                        final int moleculeStart = moleculeInterval.getStart();
                        final int moleculeEnd = moleculeInterval.getEnd();
                        intervalEnds.get(barcode).forEach(entry -> {
                            final SVInterval endInterval = entry.getInterval();

                            if (moleculeStart != (entry.getInterval().getEnd()) &&
                                    moleculeEnd != (entry.getInterval().getStart())) {
                                if (leftInterval.isUpstreamOf(endInterval)) {
                                    links.put(new PairedStrandedIntervals(
                                            new StrandedInterval(leftInterval, false),
                                            new StrandedInterval(endInterval, entry.getValue())), barcode);
                                }
                                if (rightInterval.isUpstreamOf(endInterval)) {
                                    links.put(new PairedStrandedIntervals(
                                            new StrandedInterval(rightInterval, true),
                                            new StrandedInterval(endInterval, entry.getValue())), barcode);
                                }
                            }

                        });

                    }
                    return results.iterator();
                }
        );

        final List<Tuple2<PairedStrandedIntervals, Set<Integer>>> collectedLinks = linksWithEnoughOverlappers.collect();
        writeEvidenceLinks(collectedLinks, out, contigNames, barcodeNames);
    }

    private static void writeEvidenceLinks(final List<Tuple2<PairedStrandedIntervals, Set<Integer>>> targetLinks,
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

    public static String toBedpeString(Tuple2<PairedStrandedIntervals, Set<Integer>> entry, final String[] contigNames, final String[] barcodeNames) {
        final SVInterval sourceInterval = entry._1().getLeft().getInterval();
        final SVInterval targetInterval = entry._1().getRight().getInterval();
        String sourceContigName = contigNames[sourceInterval.getContig()];
        String targetContigName = contigNames[targetInterval.getContig()];
        return sourceContigName + "\t" + (sourceInterval.getStart() - 1) + "\t" + sourceInterval.getEnd() +
                "\t" + targetContigName + "\t" + (targetInterval.getStart() - 1) + "\t" + targetInterval.getEnd() +
                "\t"  + sourceContigName + "_" + (sourceInterval.getStart() - 1) + "_" + sourceInterval.getEnd() + "_" +
                targetContigName + "_" + (targetInterval.getStart() - 1) + "_" + targetInterval.getEnd() + "_" +
                (entry._1().getLeft().getStrand() ? "P" : "M") + "_" + (entry._1().getRight().getStrand() ? "P" : "M") + "_" + entry._2().size() +
                "\t" + entry._2().size() + "\t" + (entry._1().getLeft().getStrand() ? "+" : "-") + "\t" + (entry._1.getRight().getStrand() ? "+" : "-")
                + "\t" + Utils.join(",", entry._2.stream().map(i -> barcodeNames[i]).collect(Collectors.toList()));
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