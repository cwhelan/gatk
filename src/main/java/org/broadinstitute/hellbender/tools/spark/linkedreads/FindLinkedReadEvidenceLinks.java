package org.broadinstitute.hellbender.tools.spark.linkedreads;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.broadinstitute.hellbender.tools.spark.sv.evidence.ReadMetadata;
import org.broadinstitute.hellbender.tools.spark.sv.utils.*;
import org.broadinstitute.hellbender.tools.spark.utils.IntHistogram;
import scala.Tuple2;

import java.io.File;
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

    @Override
    public boolean requiresReference() {
        return true;
    }

    @Override
    protected void runTool(final JavaSparkContext ctx) {

        logger.info("Loading linked reads");
        final ReferenceMultiSource reference = getReference();

        final Map<String, Integer> contigNameToIdMap = ReadMetadata.buildContigNameToIDMap(getReferenceSequenceDictionary());
        final String[] contigNames = ReadMetadata.buildContigIDToNameArray(contigNameToIdMap);

        final Broadcast<Map<String, Integer>> broadcastContigNameMap = ctx.broadcast(contigNameToIdMap);
        final Broadcast<String[]> broadcastContigNames = ctx.broadcast(contigNames);

        final JavaPairRDD<String, Tuple2<SVInterval, List<ReadInfo>>> barcodeIntervalsWithReads;
        barcodeIntervalsWithReads = parseBarcodeIntervals(ctx, broadcastContigNameMap, inputLinkedReads).cache();
        logger.info("Done loading linked reads");

        final IntHistogram fullIntHistogram = getGapSizeHistogram(barcodeIntervalsWithReads);

        final int median = fullIntHistogram.getCDF().median();
        logger.info("Median gap size: " + median);
        logger.info("90th: " + fullIntHistogram.getCDF().popStat(0.90f));
        logger.info("95th: " + fullIntHistogram.getCDF().popStat(0.95f));
        logger.info("99th: " + fullIntHistogram.getCDF().popStat(0.99f));

        barcodeIntervalsWithReads.unpersist();
        final JavaPairRDD<String, SVInterval> barcodeIntervalsWithoutReads = barcodeIntervalsWithReads.mapValues(Tuple2::_1);

        JavaPairRDD<String, Tuple2<SVInterval, Long>> barcodesWithoutReadsWithIds =
                barcodeIntervalsWithoutReads.zipWithUniqueId().mapToPair(pair -> new Tuple2<>(pair._1._1, new Tuple2<>(pair._1._2(), pair._2))).cache();

        final Map<String, SVIntervalTree<Tuple2<Boolean, Long>>> barcodeEndTrees = getIntervalEndsTrees(median, barcodesWithoutReadsWithIds);
        logger.info("Collected barcode ends for " + barcodeEndTrees.size() + " barcodes.");
        final Broadcast<Map<String, SVIntervalTree<Tuple2<Boolean, Long>>>> broadcastIntervalEnds = ctx.broadcast(barcodeEndTrees);

        final JavaRDD<Tuple2<PairedStrandedIntervals, Set<String>>> linksWithEnoughOverlappers = barcodesWithoutReadsWithIds.mapPartitions((iter) -> {
                    final PairedStrandedIntervalTree<Set<String>> results = new PairedStrandedIntervalTree<>();
                    final PairedStrandedIntervalTree<String> links = new PairedStrandedIntervalTree<>();
                    final Map<String, SVIntervalTree<Tuple2<Boolean, Long>>> intervalEnds = broadcastIntervalEnds.getValue();
                    while (iter.hasNext()) {
                        final Tuple2<String, Tuple2<SVInterval, Long>> next = iter.next();
                        final String barcode = next._1();
                        final Long moleculeId = next._2._2;
                        final SVInterval moleculeInterval = next._2()._1;
                        final SVInterval leftInterval = new SVInterval(
                                moleculeInterval.getContig(),
                                Math.max(0, moleculeInterval.getStart() - median),
                                moleculeInterval.getStart());
                        final SVInterval rightInterval = new SVInterval(
                                moleculeInterval.getContig(),
                                moleculeInterval.getEnd(),
                                moleculeInterval.getEnd() + median);

                        final Iterator<Tuple2<PairedStrandedIntervals, String>> overlapScanIterator = links.iterator();
                        while (overlapScanIterator.hasNext()) {
                            final Tuple2<PairedStrandedIntervals, String> link = overlapScanIterator.next();
                            final String linkBarcode = link._2();

                            if (!link._1().getLeft().getInterval().isDisjointFrom(leftInterval)) {
                                break;
                            }

                            Set<String> overlapperBarcodes = new HashSet<>();
                            overlapperBarcodes.add(linkBarcode);
                            int overlappers = 0;
                            final Iterator<Tuple2<PairedStrandedIntervals, String>> overlapIterator = links.overlappers(link._1());
                            while (overlapIterator.hasNext()) {
                                final Tuple2<PairedStrandedIntervals, String> overlapper = overlapIterator.next();
                                if (! overlapper._1().getLeft().getInterval().isDisjointFrom(leftInterval)) {
                                    break;
                                }
                                final String overlapperBarcode = overlapper._2();
                                if (!overlapperBarcode.equals(linkBarcode)) {
                                    overlapperBarcodes.add(overlapperBarcode);
                                    overlappers++;
                                }
                            }
                            if (overlappers > 3) {
                                final PairedStrandedIntervals newGoodPairedStrandedIntervals = link._1;


                                results.put(newGoodPairedStrandedIntervals, overlapperBarcodes);
                            }
                        }

                        final Iterator<Tuple2<PairedStrandedIntervals, String>> cleanupIterator = links.iterator();
                        while (cleanupIterator.hasNext()) {
                            final Tuple2<PairedStrandedIntervals, String> link = cleanupIterator.next();
                            final String linkBarcode = link._2();
                            if (linkBarcode.equals("GGCCGATAGGTTCATC-1")) {
                                System.err.println("cleaning it up");
                            }

                            if (! link._1().getLeft().getInterval().isDisjointFrom(leftInterval)) {
                                break;
                            }

                            final Iterator<Tuple2<PairedStrandedIntervals, String>> overlapIterator = links.overlappers(link._1());
                            while (overlapIterator.hasNext()) {
                                final Tuple2<PairedStrandedIntervals, String> overlapper = overlapIterator.next();
                                if (!overlapper._1().getLeft().getInterval().isDisjointFrom(leftInterval)) {
                                    break;
                                }
                            }

                            cleanupIterator.remove();
                        }

                        intervalEnds.get(barcode).forEach(entry -> {
                            final SVInterval endInterval = entry.getInterval();

                            if (!moleculeId.equals(entry.getValue()._2())) {
                                links.put(new PairedStrandedIntervals(
                                        new StrandedInterval(leftInterval, false),
                                        new StrandedInterval(endInterval, entry.getValue()._1)), barcode);
                                links.put(new PairedStrandedIntervals(
                                        new StrandedInterval(rightInterval, true),
                                        new StrandedInterval(endInterval, entry.getValue()._1)), barcode);
                            }

                        });

                    }
                    return results.iterator();
                }
        );

        linksWithEnoughOverlappers.saveAsTextFile("bar");
    }

    private Map<String, SVIntervalTree<Tuple2<Boolean, Long>>> getIntervalEndsTrees(final int median, final JavaPairRDD<String, Tuple2<SVInterval, Long>> barcodesWithoutReadsWithIds) {
        final JavaPairRDD<String, SVIntervalTree<Tuple2<Boolean, Long>>> barcodeTrees = barcodesWithoutReadsWithIds.aggregateByKey(
                null,
                (tree, pair) -> {
                    final SVInterval interval = pair._1;
                    final Long id = pair._2;
                    if (tree == null) {
                        tree = new SVIntervalTree<Tuple2<Boolean, Long>>();
                    }
                    final SVInterval leftInterval = new SVInterval(
                            interval.getContig(),
                            Math.max(0, interval.getStart() - median),
                            interval.getStart());
                    tree.put(leftInterval, new Tuple2<>(false, id));
                    final SVInterval rightInterval = new SVInterval(
                            interval.getContig(),
                            interval.getEnd(),
                            interval.getEnd() + median);
                    tree.put(rightInterval, new Tuple2<>(true, id));

                    return tree;
                },
                (tree1, tree2) -> {

                    if (tree1 == null) {
                        tree1 = new SVIntervalTree<>();
                    }
                    if (tree2 == null) {
                        tree2 = new SVIntervalTree<>();
                    }
                    final SVIntervalTree<Tuple2<Boolean, Long>> combinedTree = new SVIntervalTree<>();
                    tree1.forEach(e -> combinedTree.put(e.getInterval(), e.getValue()));
                    tree2.forEach(e -> combinedTree.put(e.getInterval(), e.getValue()));
                    return combinedTree;
                }
        );

        return barcodeTrees.collectAsMap();
    }

    private IntHistogram getGapSizeHistogram(final JavaPairRDD<String, Tuple2<SVInterval, List<ReadInfo>>> barcodeIntervalsWithReads) {
        final List<IntHistogram> partitionHistograms = barcodeIntervalsWithReads.mapPartitions(iter -> {
            IntHistogram intHistogram = new IntHistogram(MAX_TRACKED_VALUE);
            while (iter.hasNext()) {
                final Tuple2<String, Tuple2<SVInterval, List<ReadInfo>>> next = iter.next();
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

    private JavaPairRDD<String, Tuple2<SVInterval, List<ReadInfo>>> parseBarcodeIntervals(final JavaSparkContext ctx,
                                                                                          final Broadcast<Map<String, Integer>> broadcastContigNameMap,
                                                                                          final File inputLinkedReads) {
        final JavaRDD<String> stringJavaRDD = ctx.textFile(inputLinkedReads.getAbsolutePath());
        final JavaPairRDD<String, Tuple2<SVInterval, List<ReadInfo>>> moleculeIntervals =
                stringJavaRDD.mapToPair(line -> parseBarcodeIntervalLine(line, broadcastContigNameMap.getValue()));

        return moleculeIntervals;

    }

    private Tuple2<String, Tuple2<SVInterval, List<ReadInfo>>> parseBarcodeIntervalLine(final String line, final Map<String, Integer> contigNameMap) {
        final String[] fields = line.split("\t");
        int contigID = contigNameMap.get(fields[0]);
        int moleculeStart = Integer.parseInt(fields[1]);
        final SVInterval interval = new SVInterval(contigID, moleculeStart, Integer.parseInt(fields[2]));
        final String barcode = fields[3];
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

}