package org.broadinstitute.hellbender.tools.spark.linkedreads;

import com.netflix.servo.util.VisibleForTesting;
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

@CommandLineProgramProperties(
        summary = "Find interesting gaps in molecule coverage",
        oneLineSummary = "FindMoleculeGapsSpark on Spark",
        programGroup = LinkedReadsProgramGroup.class
)
public class FindMoleculeGapsSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;
    public static final int MAX_TRACKED_VALUE = 50000;

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
        final Broadcast<String[]> broadcastContigNames =  ctx.broadcast(contigNames);

        final JavaPairRDD<String, Tuple2<SVInterval, List<ReadInfo>>> barcodeIntervals;
        barcodeIntervals = parseBarcodeIntervals(ctx, broadcastContigNameMap, inputLinkedReads).cache();
        logger.info("Done loading linked reads");

        final List<IntHistogram> partitionHistograms = barcodeIntervals.mapPartitions(iter -> {
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

        final IntHistogram fullIntHistogram = partitionHistograms.stream().reduce(
                new IntHistogram(MAX_TRACKED_VALUE),
                (h1, h2) -> {
                    h1.addObservations(h2);
                    return h1;
                });

        final int median = fullIntHistogram.getCDF().median();
        logger.info("Median gap size: " + median);
        logger.info("90th: " + fullIntHistogram.getCDF().popStat(0.90f));
        logger.info("95th: " + fullIntHistogram.getCDF().popStat(0.95f));
        final int ninetyNinethPercentile = fullIntHistogram.getCDF().popStat(0.99f);
        logger.info("99th: " + ninetyNinethPercentile);
        final int rightMedianDeviation = fullIntHistogram.getCDF().rightMedianDeviation(median);
        logger.info("Right median absolute deviation: " + rightMedianDeviation);

        final int gapCutoff = ninetyNinethPercentile + 3 * rightMedianDeviation;
        final JavaPairRDD<StrandedInterval, LinkedList<Integer>> outlierGapsAtQueryPoints = barcodeIntervals.flatMapToPair(p -> {
            final Tuple2<SVInterval, List<ReadInfo>> moleculeInfo = p._2();
            final List<Tuple2<StrandedInterval, Integer>> gaps =
                    getInterestingGaps(moleculeInfo, gapCutoff);
            return gaps.iterator();
        }).aggregateByKey(null,
                (gaps, gap) -> {
                    if (gaps == null) gaps = new LinkedList<>();
                    gaps.add(Math.min(gap, MAX_TRACKED_VALUE));
                    return gaps;
                },
                (gaps1, gaps2) -> {
                    if (gaps1 == null) gaps1 = new LinkedList<>();
                    if (gaps2 == null) gaps2 = new LinkedList<>();
                    gaps1.addAll(gaps2);
                    return gaps1;
                });

        final JavaPairRDD<StrandedInterval, LinkedList<Integer>> cachedGaps = outlierGapsAtQueryPoints.cache();

        //cachedGaps.saveAsTextFile("foo1");
        final JavaPairRDD<StrandedInterval, Tuple2<Integer, Integer>> clustersAtQueryPoints = cachedGaps.flatMapToPair(kv -> {
            final StrandedInterval queryPoint = kv._1();
            final List<Integer> gapList = kv._2();
            final int[] gaps = gapList.stream().mapToInt(i -> i).toArray();
            Arrays.sort(gaps);

            final int bandwidth = 500;
            final int minClusterSize = 10;

            List<Tuple2<StrandedInterval, Tuple2<Integer, Integer>>> clusters = new ArrayList<>();
            int currentClusterStart = 0;
            int currentClusterEnd = 0;
            int gapCount = 0;
            for (int i = 0; i < gaps.length; i++) {
                final int gap = gaps[i];
                final int gapBandwidthStart = gap - bandwidth;
                final int gapBandwidthEnd = gap + bandwidth;
                if (gapBandwidthStart > currentClusterEnd) {
                    if (gapCount >= minClusterSize) {
                        clusters.add(new Tuple2<>(queryPoint, new Tuple2<>(currentClusterStart, currentClusterEnd)));
                    }
                    currentClusterStart = gapBandwidthStart;
                    currentClusterEnd = gapBandwidthEnd;
                    gapCount = 1;
                } else {
                    currentClusterEnd = gapBandwidthEnd;
                    gapCount = gapCount + 1;
                }
            }
            if (gapCount >= minClusterSize) {
                clusters.add(new Tuple2<>(queryPoint, new Tuple2<>(currentClusterStart, currentClusterEnd)));
            }

            return clusters.iterator();
        }).cache();
        //clustersAtQueryPoints.saveAsTextFile("foo2");

        final List<Tuple2<StrandedInterval, Tuple2<Integer, Integer>>> clustersAtQueryPointsLocal = clustersAtQueryPoints.collect();
        final SVIntervalTree<List<Tuple2<Boolean, Tuple2<Integer, Integer>>>> gapClusterTree = new SVIntervalTree<>();
        clustersAtQueryPointsLocal.forEach(kv -> {
            if (gapClusterTree.find(kv._1.getInterval()) == null) {
                gapClusterTree.put(kv._1.getInterval(), new ArrayList<>());
            }
            gapClusterTree.find(kv._1.getInterval()).getValue().add(new Tuple2<>(kv._1.getStrand(), kv._2));
        });

        final Broadcast<SVIntervalTree<List<Tuple2<Boolean, Tuple2<Integer, Integer>>>>> broadcastGapClusterTree = ctx.broadcast(gapClusterTree);

        final JavaPairRDD<String, Tuple2<SVInterval, List<ReadInfo>>> splitIntervals = barcodeIntervals.flatMapToPair(kv -> {
            final String barcode = kv._1;
            final SVInterval interval = kv._2()._1();
            final List<ReadInfo> reads = kv._2()._2();

            final List<Tuple2<String, Tuple2<SVInterval, List<ReadInfo>>>> results = splitMoleculesForBarcode(barcode, interval, reads, broadcastGapClusterTree.getValue());

            return results.iterator();
        });

        final JavaPairRDD<SVInterval, String> bedRecordsByBarcode = splitIntervals.mapToPair(kv -> {
            final String barcode = kv._1();
            final Tuple2<SVInterval, List<ReadInfo>> intervalWithReads = kv._2();
            final SVInterval svInterval = intervalWithReads._1();
            return new Tuple2<>(svInterval, ExtractLinkedReadsSpark.intervalTreeToBedRecord(barcode, broadcastContigNames.getValue(), svInterval, intervalWithReads._2()));
        });

        if (shardedOutput) {
            bedRecordsByBarcode.values().saveAsTextFile(out);
        } else {
            final String shardedOutputDirectory = out + ".parts";
            final int numParts = bedRecordsByBarcode.getNumPartitions();
            bedRecordsByBarcode.sortByKey().values().saveAsTextFile(shardedOutputDirectory);
            ExtractLinkedReadsSpark.unshardOutput(out, shardedOutputDirectory, numParts);
        }

    }

    @VisibleForTesting
    static List<Tuple2<String, Tuple2<SVInterval, List<ReadInfo>>>> splitMoleculesForBarcode(final String barcode,
                                                                                              final SVInterval interval,
                                                                                              final List<ReadInfo> reads,
                                                                                              final SVIntervalTree<List<Tuple2<Boolean, Tuple2<Integer, Integer>>>> gapTree) {
        final List<Tuple2<String, Tuple2<SVInterval, List<ReadInfo>>>> results = new ArrayList<>(1);

        final int binsize = 1000;

        ReadInfo prevReadInfo = reads.get(0);
        int prevBin = prevReadInfo.getStart() - prevReadInfo.getStart() % binsize;

        int prevSplit = 0;

        for (int i = 1; i < reads.size(); i++)  {
            final ReadInfo read = reads.get(i);
            final int bin = read.getStart() - read.getStart() % binsize;

            if (prevBin != bin) {
                final int gapSize = read.getStart() - prevReadInfo.getStart();

                final SVInterval prevBinInterval = new SVInterval(read.contig, prevBin, prevBin + binsize);
                final SVInterval newBinInterval = new SVInterval(read.contig, bin, bin + binsize);
                final SVIntervalTree.Entry<List<Tuple2<Boolean, Tuple2<Integer, Integer>>>> prevClustersEntry = gapTree.find(prevBinInterval);
                final SVIntervalTree.Entry<List<Tuple2<Boolean, Tuple2<Integer, Integer>>>> newClustersEntry = gapTree.find(newBinInterval);
                boolean match = false;
                if (prevClustersEntry != null) {
                    for (final Tuple2<Boolean, Tuple2<Integer, Integer>> cluster : prevClustersEntry.getValue()) {
                        if (cluster._1() && gapSize >= cluster._2()._1() && gapSize <= cluster._2()._2()) {
                            match = true;
                        }
                    }
                }
                if (newClustersEntry != null) {
                    for (final Tuple2<Boolean, Tuple2<Integer, Integer>> cluster : newClustersEntry.getValue()) {
                        if (!cluster._1() && gapSize >= cluster._2()._1() && gapSize <= cluster._2()._2()) {
                            match = true;
                        }
                    }
                }
                if (match) {
                    final List<ReadInfo> readsToSplitOff = reads.subList(prevSplit, i);
                    final SVInterval newInterval = new SVInterval(interval.getContig(), readsToSplitOff.get(0).getStart(), readsToSplitOff.get(readsToSplitOff.size() - 1).getEnd());
                    results.add(new Tuple2<>(barcode, new Tuple2<>(newInterval, readsToSplitOff)));
                    prevSplit = i;
                }

            }

            prevReadInfo = read;
            prevBin = prevReadInfo.getStart() - prevReadInfo.getStart() % binsize;
        }
        final List<ReadInfo> remainingReads = reads.subList(prevSplit, reads.size());
        final SVInterval newInterval = new SVInterval(interval.getContig(), remainingReads.get(0).getStart(), remainingReads.get(remainingReads.size() - 1).getEnd());
        results.add(new Tuple2<>(barcode, new Tuple2<>(newInterval, remainingReads)));

        return results;
    }

    static List<Tuple2<StrandedInterval, Integer>> getInterestingGaps(final Tuple2<SVInterval, List<ReadInfo>> moleculeInfo, final int minSize) {
        final SVInterval moleculeInterval = moleculeInfo._1();
        final List<ReadInfo> readInfos = moleculeInfo._2();

        final int binsize = 1000;

        final List<Tuple2<StrandedInterval, Integer>> gaps = new ArrayList<>(moleculeInterval.getLength() / binsize);

        ReadInfo prevReadInfo = readInfos.get(0);
        int prevBin = prevReadInfo.getStart() - prevReadInfo.getStart() % binsize;
        for (int i = 1; i < readInfos.size(); i++)  {
            final ReadInfo readInfo = readInfos.get(i);
            final int bin = readInfo.getStart() - readInfo.getStart() % binsize;
            if (bin != prevBin) {
                final int gapSize = readInfo.getStart() - prevReadInfo.getStart();

                if (gapSize >= minSize) {
                    final StrandedInterval startStrandedInterval = new StrandedInterval(new SVInterval(readInfo.contig, prevBin, prevBin + binsize), true);
                    final StrandedInterval endStrandedInterval = new StrandedInterval(new SVInterval(readInfo.contig, bin, bin + binsize), false);

                    gaps.add(new Tuple2<>(startStrandedInterval, gapSize));
                    gaps.add(new Tuple2<>(endStrandedInterval, gapSize));
                }
            }
            prevReadInfo = readInfo;
            prevBin = bin;

        }
        return gaps;
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
