package org.broadinstitute.hellbender.tools.spark.linkedreads;

import com.esotericsoftware.kryo.DefaultSerializer;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.annotations.VisibleForTesting;
import htsjdk.samtools.util.BlockCompressedOutputStream;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.ArgumentCollection;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.LinkedReadsProgramGroup;
import org.broadinstitute.hellbender.engine.filters.ReadFilter;
import org.broadinstitute.hellbender.engine.spark.GATKSparkTool;
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.spark.sv.StructuralVariationDiscoveryArgumentCollection;
import org.broadinstitute.hellbender.tools.spark.sv.evidence.FindBreakpointEvidenceSpark;
import org.broadinstitute.hellbender.tools.spark.sv.evidence.ReadMetadata;
import org.broadinstitute.hellbender.tools.spark.sv.evidence.SVReadFilter;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVInterval;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVIntervalTree;
import org.broadinstitute.hellbender.utils.Utils;
import org.broadinstitute.hellbender.utils.gcs.BucketUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.seqdoop.hadoop_bam.util.NIOFileUtil;
import scala.Tuple2;

import java.io.*;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.stream.Collectors;

@CommandLineProgramProperties(
        summary = "Computes the coverage by long molecules from linked-read data",
        oneLineSummary = "CollectLinkedReadCoverage on Spark",
        programGroup = LinkedReadsProgramGroup.class
)
public class ExtractLinkedReadsSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;

    @Argument(doc = "uri for the output file",
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME, fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
            optional = true)
    public String out;

    @Argument(doc = "cluster size",
            shortName = "cluster-size", fullName = "cluster-size",
            optional = true)
    public int clusterSize = 25000;

    @Argument(doc = "molSizeHistogramFile",
            shortName = "mol-size-histogram", fullName = "molecule-size-histogram-file",
            optional = true)
    public String molSizeHistogramFile;

    @Argument(doc = "gapHistogramFile",
            shortName = "gap-histogram-file", fullName = "gap-histogram-file",
            optional = true)
    public String gapHistogramFile;

    @Argument(doc = "metadataFile",
            shortName = "metadata-file", fullName = "metadata-file",
            optional = true)
    public String metadataFile;

    @Argument(doc = "barcodeFragmentCountsFile",
            shortName = "barcode-fragment-counts-file", fullName = "barcode-fragment-counts-file",
            optional = true)
    public String barcodeFragmentCountsFile;


    @Argument(doc = "phase-set-intervals-file",
            shortName = "phase-set-intervals-file", fullName = "phase-set-intervals-file",
            optional = true)
    public String phaseSetIntervalsFile;

    @Argument(fullName = "min-read-count-per-molecule", shortName = "min-read-count-per-molecule", doc="Minimum number of reads to call a molecule", optional=true)
    public int minReadCountPerMol = 2;

    @Argument(fullName = "edge-read-mapq-threshold", shortName = "edge-read-mapq-threshold", doc="Mapq below which to trim reads from starts and ends of molecules", optional=true)
    public int edgeReadMapqThreshold = 30;

    @Argument(fullName = "min-max-mapq", shortName = "min-max-mapq", doc="Minimum highest mapq read to create a fragment", optional=true)
    public int minMaxMapq = 30;

    @Argument(fullName = "filter-high-depth", shortName = "filter-high-depth", doc="Filter out high-depth regions as defined by high-depth-coverage-peak-factor and high-depth-coverage-factor", optional=true)
    public boolean filterHighDepth = false;

    @Argument(doc = "Largest fragment size that will be explicitly counted in determining " +
            "fragment size statistics.", fullName = "max-tracked-fragment-length")
    public int maxTrackedFragmentLength = 2000;

    @Argument(doc = "We filter out contiguous regions of the genome that have coverage of at least high-depth-coverage-factor * avg-coverage and a " +
            "peak coverage of high-depth-coverage-peak-factor * avg-coverage, because the reads mapped to those regions tend to be non-local and high depth prevents accurate assembly.",
            fullName = "high-depth-coverage-factor")
    public int highDepthCoverageFactor = 4;

    @Argument(doc = "We filter out contiguous regions of the genome that have coverage of at least high-depth-coverage-factor * avg-coverage and a " +
            "peak coverage of high-depth-coverage-peak-factor * avg-coverage, because the reads mapped to those regions tend to be non-local and high depth prevents accurate assembly.",
            fullName = "high-depth-coverage-peak-factor")
    public int highDepthCoveragePeakFactor = 10;


    @Argument(doc = "file for high-coverage intervals output", fullName = "high-coverage-intervals", optional = true)
    public String highCoverageIntervalsFile;

    @Override
    public boolean requiresReads() { return true; }

    @Override
    public boolean requiresReference() { return true; }

    @Override
    public ReadFilter makeReadFilter() {
        return super.makeReadFilter().
                and(new LinkedReadAnalysisFilter());
    }

    @Override
    protected void runTool(final JavaSparkContext ctx) {
        final JavaRDD<GATKRead> reads = getReads();

        final int finalClusterSize = clusterSize;

        final JavaRDD<GATKRead> mappedReads =
                reads.filter(read ->
                        !read.isDuplicate() && !read.failsVendorQualityCheck() && !read.isUnmapped() && ! read.isSecondaryAlignment() && ! read.isSupplementaryAlignment());

        final Map<String, Integer> contigNameToIdMap = ReadMetadata.buildContigNameToIDMap(getHeaderForReads().getSequenceDictionary());
        final String[] contigNames = ReadMetadata.buildContigIDToNameArray(contigNameToIdMap);

        final StructuralVariationDiscoveryArgumentCollection.FindBreakpointEvidenceSparkArgumentCollection params =
                new StructuralVariationDiscoveryArgumentCollection.FindBreakpointEvidenceSparkArgumentCollection();
        params.highDepthCoverageFactor = highDepthCoverageFactor;
        params.highDepthCoveragePeakFactor = highDepthCoveragePeakFactor;
        params.highCoverageIntervalsFile = highCoverageIntervalsFile;

        final SVIntervalTree<SVInterval> highCoverageSubintervalTree;
        if (filterHighDepth) {
            final SVReadFilter filter = new SVReadFilter(params);

            final ReadMetadata readMetadata =
                    new ReadMetadata(Collections.emptySet(), getHeaderForReads(), maxTrackedFragmentLength, getUnfilteredReads(), filter, logger);
            final Broadcast<ReadMetadata> broadcastMetadata = ctx.broadcast(readMetadata);

            highCoverageSubintervalTree = FindBreakpointEvidenceSpark.findGenomewideHighCoverageIntervalsToIgnore(params, readMetadata, ctx,
                    getHeaderForReads(), getUnfilteredReads(), filter, logger, broadcastMetadata);

            broadcastMetadata.destroy();
        } else {
            highCoverageSubintervalTree = new SVIntervalTree<>();
        }

        Broadcast<SVIntervalTree<SVInterval>> broadcastHighDepthIntervals = ctx.broadcast(highCoverageSubintervalTree);
        logger.info("Loading linked reads");
        final Broadcast<Map<String, Integer>> broadcastContigNameMap = ctx.broadcast(contigNameToIdMap);
        final Broadcast<String[]> broadcastContigNames =  ctx.broadcast(contigNames);

        final JavaPairRDD<String, SVIntervalTree<List<ReadInfo>>> barcodeIntervals
            = getBarcodeIntervals(finalClusterSize, mappedReads, broadcastContigNameMap, minReadCountPerMol, minMaxMapq, edgeReadMapqThreshold, broadcastHighDepthIntervals)
                .repartition(ctx.defaultParallelism()).cache();

        if (barcodeFragmentCountsFile != null) {
            computeFragmentCounts(barcodeIntervals, barcodeFragmentCountsFile);
        }

        if (molSizeHistogramFile != null) {
            computeMolSizeHistogram(barcodeIntervals, molSizeHistogramFile);
        }

        if (gapHistogramFile != null) {
            computeGapSizeHistogram(barcodeIntervals, gapHistogramFile);
        }

        if (out != null) {
            writeIntervalsAsBed12(broadcastContigNames, barcodeIntervals, shardedOutput, out);
        }

        if (phaseSetIntervalsFile != null) {
            final JavaRDD<String> phaseSetIntervals = mappedReads.mapPartitions(iter -> {
                List<Tuple2<Integer, SVInterval>> partitionPhaseSets = new ArrayList<>();
                int currentPS = -1;
                int currentContig = -1;
                int currentStart = -1;
                int currentEnd = -1;
                while (iter.hasNext()) {
                    GATKRead read = iter.next();
                    if (read.hasAttribute("PS")) {
                        int ps = read.getAttributeAsInteger("PS");
                        if (ps != currentPS) {
                            if (currentPS != -1) {
                                partitionPhaseSets.add(new Tuple2<>(currentPS, new SVInterval(currentContig, currentStart, currentEnd)));
                            }
                            currentPS = ps;
                            currentContig = broadcastContigNameMap.getValue().get(read.getContig());
                            currentStart = read.getStart();
                            currentEnd = read.getEnd();
                        } else {
                            currentEnd = read.getEnd();
                        }
                    }
                }
                return partitionPhaseSets.iterator();
            }).mapToPair(v -> v).reduceByKey((svInterval1, svInterval2) -> new SVInterval(svInterval1.getContig(),
                    Math.min(svInterval1.getStart(), svInterval2.getStart()),
                    Math.max(svInterval1.getEnd(), svInterval2.getEnd())))
                    .mapToPair(kv -> new Tuple2<>(kv._2(), kv._1())).sortByKey()
                    .map(kv -> {
                        final SVInterval svInterval = kv._1();
                        final Integer ps = kv._2();
                        final String contigName = broadcastContigNames.getValue()[svInterval.getContig()];
                        return contigName + "\t" + svInterval.getStart() + "\t" + svInterval.getEnd() + "\t" + ps;
                    });


            if (shardedOutput) {
                phaseSetIntervals.saveAsTextFile(phaseSetIntervalsFile);
            } else {
                final String shardedOutputDirectory = phaseSetIntervalsFile + ".parts";
                phaseSetIntervals.saveAsTextFile(shardedOutputDirectory);
                unshardOutput(phaseSetIntervalsFile, shardedOutputDirectory, phaseSetIntervals.getNumPartitions());
            }
        }

    }


    private static void writeIntervalsAsBed12(final Broadcast<String[]> contigNames,
                                              final JavaPairRDD<String, SVIntervalTree<List<ReadInfo>>> barcodeIntervals,
                                              final boolean shardedOutput,
                                              final String out) {
        final JavaPairRDD<SVInterval, String> bedRecordsByBarcode;
        bedRecordsByBarcode = barcodeIntervals.flatMapToPair(x -> {
            final String barcode = x._1;
            final SVIntervalTree<List<ReadInfo>> svIntervalTree = x._2;

            final List<Tuple2<SVInterval, String>> results = new ArrayList<>();
            for (final SVIntervalTree.Entry<List<ReadInfo>> next : svIntervalTree) {
                results.add(new Tuple2<>(next.getInterval(), intervalTreeToBedRecord(barcode, next, contigNames.getValue())));
            }

            return results.iterator();
        });

        if (shardedOutput) {
            bedRecordsByBarcode.values().saveAsTextFile(out);
        } else {
            final String shardedOutputDirectory = out + ".parts";
            final int numParts = bedRecordsByBarcode.getNumPartitions();
            bedRecordsByBarcode.sortByKey().values().saveAsTextFile(shardedOutputDirectory);
            //final BlockCompressedOutputStream outputStream = new BlockCompressedOutputStream(out);
            unshardOutput(out, shardedOutputDirectory, numParts);
        }
    }

    private static void unshardOutput(final String out, final String shardedOutputDirectory, final int numParts) {
        final OutputStream outputStream;

        outputStream = new BlockCompressedOutputStream(new BufferedOutputStream(BucketUtils.createFile(out)), null);
        for (int i = 0; i < numParts; i++) {
            String fileName = String.format("part-%1$05d", i);
            try {
                Path path = NIOFileUtil.asPath(shardedOutputDirectory + System.getProperty("file.separator") + fileName);
                if (Files.exists(path)) {
                    final BufferedInputStream bufferedInputStream = new BufferedInputStream(Files.newInputStream(path));
                    int bite;
                    while ((bite = bufferedInputStream.read()) != -1) {
                        outputStream.write(bite);
                    }
                    bufferedInputStream.close();
                }

            } catch (IOException e) {
                throw new GATKException(e.getMessage());
            }
        }
        try {
            outputStream.close();
        } catch (IOException e) {
            throw new GATKException(e.getMessage());
        }
        try {
            deleteRecursive(NIOFileUtil.asPath(shardedOutputDirectory));
        } catch (IOException e) {
            throw new GATKException(e.getMessage());
        }
    }

    private static void computeGapSizeHistogram(final JavaPairRDD<String, SVIntervalTree<List<ReadInfo>>> barcodeIntervals, final String gapHistogramFile) {
        final Tuple2<double[], long[]> readGapHistogram = barcodeIntervals.flatMapToDouble(kv -> {
            final SVIntervalTree<List<ReadInfo>> intervalTree = kv._2();
            List<Double> results = new ArrayList<>();
            for (final SVIntervalTree.Entry<List<ReadInfo>> next : intervalTree) {
                List<ReadInfo> readInfoList = next.getValue();
                readInfoList.sort(Comparator.comparing(ReadInfo::getStart));
                for (int i = 1; i < readInfoList.size(); i++) {
                    results.add((double) (readInfoList.get(i).start - readInfoList.get(i - 1).start));
                }
            }
            return results.iterator();
        }).histogram(1000);


        try (final Writer writer =
                     new BufferedWriter(new OutputStreamWriter(BucketUtils.createFile(gapHistogramFile)))) {
            writer.write("# Read gap histogram\n");
            for (int i = 1; i < readGapHistogram._1().length; i++) {
                writer.write(readGapHistogram._1()[i - 1] + "-" + readGapHistogram._1()[i] + "\t" + readGapHistogram._2()[i - 1] + "\n");
            }
        } catch (final IOException ioe) {
            throw new GATKException("Can't write gap histogram file.", ioe);
        }
    }

    private static Tuple2<double[], long[]> computeMolSizeHistogram(final JavaPairRDD<String, SVIntervalTree<List<ReadInfo>>> barcodeIntervals, final String molSizeHistogramFile) {
        final JavaDoubleRDD moleculeSizes = barcodeIntervals.flatMapToDouble(kv -> {
            final SVIntervalTree<List<ReadInfo>> intervalTree = kv._2();
            List<Double> results = new ArrayList<>(intervalTree.size());
            final Iterator<SVIntervalTree.Entry<List<ReadInfo>>> iterator = intervalTree.iterator();
            Utils.stream(iterator).map(e -> e.getInterval().getLength()).forEach(l -> results.add(new Double(l)));
            return results.iterator();
        });

        final Tuple2<double[], long[]> moleculeLengthHistogram = moleculeSizes.histogram(1000);

        try (final Writer writer =
                     new BufferedWriter(new OutputStreamWriter(BucketUtils.createFile(molSizeHistogramFile)))) {
            writer.write("# Molecule length histogram\n");
            for (int i = 1; i < moleculeLengthHistogram._1().length; i++) {
                writer.write(moleculeLengthHistogram._1()[i - 1] + "-" + moleculeLengthHistogram._1()[i] + "\t" + moleculeLengthHistogram._2()[i - 1] + "\n");
            }
        } catch (final IOException ioe) {
            throw new GATKException("Can't write read histogram file.", ioe);
        }

        return moleculeLengthHistogram;
    }

    private static void computeFragmentCounts(final JavaPairRDD<String, SVIntervalTree<List<ReadInfo>>> barcodeIntervals, final String barcodeFragmentCountsFile) {
        final JavaPairRDD<Integer, String> barcodeReadCounts = barcodeIntervals.mapToPair(kv ->
                new Tuple2<>(kv._1(), Utils.stream(kv._2().iterator()).mapToInt(e -> e.getValue().size()).sum())).mapToPair(Tuple2::swap).sortByKey(true, 1);
        barcodeReadCounts.saveAsTextFile(barcodeFragmentCountsFile);
    }

    private static JavaPairRDD<String, SVIntervalTree<List<ReadInfo>>> getBarcodeIntervals(final int finalClusterSize,
                                                                                           final JavaRDD<GATKRead> mappedReads,
                                                                                           final Broadcast<Map<String, Integer>> broadcastContigNameMap,
                                                                                           final int minReadCountPerMol,
                                                                                           final int minMaxMapq,
                                                                                           final int edgeReadMapqThreshold, final Broadcast<SVIntervalTree<SVInterval>> broadcastHighDepthIntervals) {
        final JavaPairRDD<String, SVIntervalTree<List<ReadInfo>>> readsClusteredByBC = getClusteredReadIntervalsByTag(finalClusterSize, mappedReads, broadcastContigNameMap, "BX", broadcastHighDepthIntervals);
        final JavaPairRDD<String, SVIntervalTree<List<ReadInfo>>> overlappersRemovedClusteredReads = overlapperFilter(readsClusteredByBC, finalClusterSize);
        final JavaPairRDD<String, SVIntervalTree<List<ReadInfo>>> edgefilteredClusteredReads = edgeFilterFragments(overlappersRemovedClusteredReads, edgeReadMapqThreshold);
        final JavaPairRDD<String, SVIntervalTree<List<ReadInfo>>> minMoleculeCountFragments = minMoleculeCountFilterFragments(edgefilteredClusteredReads, minReadCountPerMol);
        final JavaPairRDD<String, SVIntervalTree<List<ReadInfo>>> maxMapqFilteredFragments = minMaxMapqFilterFragments(minMoleculeCountFragments, minMaxMapq);
        return maxMapqFilteredFragments;
    }

    private static JavaPairRDD<String, SVIntervalTree<List<ReadInfo>>> overlapperFilter(final JavaPairRDD<String, SVIntervalTree<List<ReadInfo>>> readsClusteredByBC,
                                                                                        final int finalClusterSize) {
        // todo: split up edge cases where this filtering has created a gap greater than allowed by clustersize
        return readsClusteredByBC.mapValues(ExtractLinkedReadsSpark::cleanOverlappingTreeEntries);
    }

    @VisibleForTesting
    static SVIntervalTree<List<ReadInfo>> cleanOverlappingTreeEntries(final SVIntervalTree<List<ReadInfo>> tree) {
        final List<Tuple2<SVInterval, List<ReadInfo>>> newEntries = new ArrayList<>(tree.size());
        final Iterator<SVIntervalTree.Entry<List<ReadInfo>>> iterator = tree.iterator();

        while (iterator.hasNext()) {
            SVIntervalTree.Entry<List<ReadInfo>> entry = iterator.next();
            List<ReadInfo> reads = entry.getValue();
            List<ReadInfo> newList = new ArrayList<>(reads.size());
            Collections.sort(reads, Comparator.comparingInt(ReadInfo::getStart));

            int currentEnd = -1;
            int bestOverlapperMapqIdx = -1;
            final List<ReadInfo> overlappers = new ArrayList<>();
            for (ReadInfo readInfo : reads) {
                if (overlappers.isEmpty() || readInfo.getStart() < currentEnd) {
                    overlappers.add(readInfo);
                    if (bestOverlapperMapqIdx == -1 || readInfo.getMapq() > overlappers.get(bestOverlapperMapqIdx).getMapq()) {
                        bestOverlapperMapqIdx = overlappers.size() - 1;
                    }
                    currentEnd = readInfo.getEnd();
                } else {
                    if (! overlappers.isEmpty()) {
                        final ReadInfo bestOverlapper = overlappers.get(bestOverlapperMapqIdx);
                        overlappers.clear();
                        bestOverlapperMapqIdx = -1;
                        newList.add(bestOverlapper);
                    }
                    newList.add(readInfo);
                    currentEnd = readInfo.getEnd();
                }
            }

            if (! overlappers.isEmpty()) {
                final ReadInfo bestOverlapper = overlappers.get(bestOverlapperMapqIdx);
                newList.add(bestOverlapper);
            }

            newEntries.add(new Tuple2<>(entry.getInterval(), newList));
            iterator.remove();
        }

        newEntries.forEach(p -> tree.put(p._1(), p._2()));
        return tree;
    }

    private static JavaPairRDD<String, SVIntervalTree<List<ReadInfo>>> minMoleculeCountFilterFragments(final JavaPairRDD<String, SVIntervalTree<List<ReadInfo>>> clusteredReads, final int minReadCountPerMol) {
        if (minReadCountPerMol > 0) {
            final JavaPairRDD<String, SVIntervalTree<List<ReadInfo>>> filteredIntervalsByKey = clusteredReads
                    .mapValues(intervalTree -> {
                        final Iterator<SVIntervalTree.Entry<List<ReadInfo>>> iterator = intervalTree.iterator();
                        while (iterator.hasNext()) {
                            final SVIntervalTree.Entry<List<ReadInfo>> next = iterator.next();
                            if (next.getValue().size() < minReadCountPerMol) {
                                iterator.remove();
                            }
                        }
                        return intervalTree;
                    })
                    .filter(kv -> {
                        final SVIntervalTree<List<ReadInfo>> intervalTree = kv._2();
                        return intervalTree.size() > 0;
                    });
            return filteredIntervalsByKey;
        } else {
            return clusteredReads;
        }
    }

    private static JavaPairRDD<String, SVIntervalTree<List<ReadInfo>>> minMaxMapqFilterFragments(final JavaPairRDD<String, SVIntervalTree<List<ReadInfo>>> readsClusteredByBC, final int minMaxMapq) {
        if (minMaxMapq > 0) {
            return readsClusteredByBC.flatMapToPair(kv -> {
                final SVIntervalTree<List<ReadInfo>> intervalTree = kv._2();
                final Iterator<SVIntervalTree.Entry<List<ReadInfo>>> treeIterator = intervalTree.iterator();
                while (treeIterator.hasNext()) {
                    SVIntervalTree.Entry<List<ReadInfo>> entry = treeIterator.next();
                    final List<ReadInfo> value = entry.getValue();
                    if (!value.stream().anyMatch(readInfo -> readInfo.getMapq() >= minMaxMapq)) {
                        treeIterator.remove();
                    }
                }
                if (intervalTree.size() == 0) {
                    return Collections.emptyIterator();
                } else {
                    return Collections.singletonList(new Tuple2<>(kv._1(), intervalTree)).iterator();
                }
            });
        } else {
            return readsClusteredByBC;
        }

    }

    private static JavaPairRDD<String, SVIntervalTree<List<ReadInfo>>> edgeFilterFragments(final JavaPairRDD<String, SVIntervalTree<List<ReadInfo>>> readsClusteredByBC, final int edgeReadMapqThreshold) {
        if (edgeReadMapqThreshold > 0) {
            return readsClusteredByBC.flatMapToPair(kv -> {
                final SVIntervalTree<List<ReadInfo>> myTree = edgeTrimTree(edgeReadMapqThreshold, kv._2());
                if (myTree.size() == 0) {
                    return Collections.emptyIterator();
                } else {
                    return Collections.singletonList(new Tuple2<>(kv._1(), myTree)).iterator();
                }
            });
        } else {
            return readsClusteredByBC;
        }

    }

    static SVIntervalTree<List<ReadInfo>> edgeTrimTree(final int edgeReadMapqThreshold, final SVIntervalTree<List<ReadInfo>> myTree) {
        final Iterator<SVIntervalTree.Entry<List<ReadInfo>>> treeIterator = myTree.iterator();
        final List<List<ReadInfo>> modifiedValues = new ArrayList<>(myTree.size());
        while (treeIterator.hasNext()) {
            SVIntervalTree.Entry<List<ReadInfo>> entry =  treeIterator.next();
            boolean trimmed = false;
            final List<ReadInfo> value = entry.getValue();
            final Iterator<ReadInfo> iterator = value.iterator();
            while (iterator.hasNext()) {
                final ReadInfo readInfo = iterator.next();
                if (readInfo.getMapq() > edgeReadMapqThreshold) {
                    break;
                } else {
                    iterator.remove();
                    trimmed = true;
                }
            }
            final ListIterator<ReadInfo> revIterator = value.listIterator(value.size());
            while (revIterator.hasPrevious()) {
                final ReadInfo readInfo = revIterator.previous();
                if (readInfo.getMapq() > edgeReadMapqThreshold) {
                    break;
                } else {
                    revIterator.remove();
                    trimmed = true;
                }
            }
            if (trimmed) {
                treeIterator.remove();
                if (!entry.getValue().isEmpty()) {
                    modifiedValues.add(entry.getValue());
                }
            }
        }
        modifiedValues.forEach(l -> {
            final SVInterval interval = new SVInterval(l.get(0).contig, l.get(0).start, l.get(l.size() - 1).end);
            myTree.put(interval, l);
        });
        return myTree;
    }

    private static JavaPairRDD<String, SVIntervalTree<List<ReadInfo>>> getClusteredReadIntervalsByTag(final int finalClusterSize,
                                                                                                      final JavaRDD<GATKRead> mappedReads,
                                                                                                      final Broadcast<Map<String, Integer>> broadcastContigNameMap,
                                                                                                      final String tag, final Broadcast<SVIntervalTree<SVInterval>> broadcastHighDepthIntervals) {

        final JavaPairRDD<String, SVIntervalTree<List<ReadInfo>>> intervalsByKey = mappedReads.filter(GATKRead::isFirstOfPair)
                .filter(read -> highDepthRegionFilter(broadcastContigNameMap, broadcastHighDepthIntervals, read))
                .mapToPair(read -> new Tuple2<>(read.getAttributeAsString(tag), new ReadInfo(broadcastContigNameMap.getValue(), read)))
                .combineByKey(
                        readInfo -> {
                            SVIntervalTree<List<ReadInfo>> intervalTree = new SVIntervalTree<>();
                            return addReadToIntervals(intervalTree, readInfo, finalClusterSize);
                        }
                        ,
                        (aggregator, read) -> addReadToIntervals(aggregator, read, finalClusterSize),
                        (intervalTree1, intervalTree2) -> combineIntervalLists(intervalTree1, intervalTree2, finalClusterSize)
                );

        return intervalsByKey;
    }

    private static Boolean highDepthRegionFilter(final Broadcast<Map<String, Integer>> broadcastContigNameMap, final Broadcast<SVIntervalTree<SVInterval>> broadcastHighDepthIntervals, final GATKRead read) {
        if (broadcastHighDepthIntervals.getValue() != null) {
            final int readContigId = broadcastContigNameMap.getValue().get(read.getContig());
            final SVInterval clippedReadInterval = new SVInterval(readContigId, read.getStart(), read.getEnd());
            return ! SVReadFilter.containedInRegionToIgnore(clippedReadInterval, broadcastHighDepthIntervals.getValue());
        } else {
            return true;
        }
    }

    /**
     * Delete the given directory and all of its contents if non-empty.
     * @param directory the directory to delete
     */
    private static void deleteRecursive(Path directory) throws IOException {
        Files.walkFileTree(directory, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }
            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }


    @VisibleForTesting
    static SVIntervalTree<List<ReadInfo>> addReadToIntervals(final SVIntervalTree<List<ReadInfo>> inputIntervalTree, final ReadInfo read, final int clusterSize) {
        final SVInterval sloppedReadInterval = new SVInterval(read.getContig(), read.getStart() - clusterSize, read.getEnd()+clusterSize);
        SVIntervalTree<List<ReadInfo>> myTree = inputIntervalTree;
        if (inputIntervalTree == null) {
            myTree = new SVIntervalTree<>();
        }
        final Iterator<SVIntervalTree.Entry<List<ReadInfo>>> iterator = myTree.overlappers(
                sloppedReadInterval);
        int start = read.getStart();
        int end = read.getEnd();
        final List<ReadInfo> newReadList = new ArrayList<>();
        newReadList.add(read);
        if (iterator.hasNext()) {
            final SVIntervalTree.Entry<List<ReadInfo>> existingNode = iterator.next();
            final int currentStart = existingNode.getInterval().getStart();
            final int currentEnd = existingNode.getInterval().getEnd();
            final List<ReadInfo> currentValue = existingNode.getValue();
            start = Math.min(currentStart, read.getStart());
            end = Math.max(currentEnd, read.getEnd());
            newReadList.addAll(currentValue);
            iterator.remove();
        }
        while (iterator.hasNext()) {
            final SVIntervalTree.Entry<List<ReadInfo>> next = iterator.next();
            final int currentEnd = next.getInterval().getStart();
            final List<ReadInfo> currentValue = next.getValue();
            end = Math.max(end, currentEnd);
            newReadList.addAll(currentValue);
            iterator.remove();
        }
        myTree.put(new SVInterval(read.getContig(), start, end), newReadList);
        return myTree;
    }

    private static SVIntervalTree<List<ReadInfo>> combineIntervalLists(final SVIntervalTree<List<ReadInfo>> intervalTree1,
                                                                       final SVIntervalTree<List<ReadInfo>> intervalTree2,
                                                                       final int clusterSize) {
        return mergeIntervalTrees(intervalTree1, intervalTree2, clusterSize);

    }

    @VisibleForTesting
    static SVIntervalTree<List<ReadInfo>> mergeIntervalTrees(final SVIntervalTree<List<ReadInfo>> tree1, final SVIntervalTree<List<ReadInfo>> tree2, final int clusterSize) {

        if (tree1 == null || tree1.size() == 0) return tree2;
        if (tree2 == null || tree2.size() == 0) return tree1;

        final SVIntervalTree<List<ReadInfo>> mergedTree = new SVIntervalTree<>();

        PriorityQueue<SVIntervalTree.Entry<List<ReadInfo>>> nodes = new PriorityQueue<>(tree1.size() + tree2.size(),
                (o1, o2) -> {
                    if (o1.getInterval().getContig() == o2.getInterval().getContig()) {
                        return new Integer(o1.getInterval().getStart()).compareTo(o2.getInterval().getStart());
                    } else {
                        return new Integer(o1.getInterval().getContig()).compareTo(o2.getInterval().getContig());
                    }
                });

        tree1.iterator().forEachRemaining(nodes::add);
        tree2.iterator().forEachRemaining(nodes::add);

        int currentContig = -1;
        int currentStart = -1;
        int currentEnd = -1;
        List<ReadInfo> values = new ArrayList<>();
        while (nodes.size() > 0) {
            final SVIntervalTree.Entry<List<ReadInfo>> next = nodes.poll();
            final SVInterval newInterval = next.getInterval();
            final int newContig = newInterval.getContig();
            if (currentContig != newContig) {
                if (currentContig != -1) {
                    mergedTree.put(new SVInterval(currentContig, currentStart, currentEnd), values);
                }
                currentContig = newContig;
                currentStart = newInterval.getStart();
                currentEnd = newInterval.getEnd();
                values = new ArrayList<>(next.getValue());
            } else {
                if (overlaps(newInterval, currentEnd, clusterSize)) {
                    currentEnd = Math.max(currentEnd, newInterval.getEnd());
                    values.addAll(next.getValue());
                } else {
                    // no overlap, so put the previous node in and set up the next set of values
                    mergedTree.put(new SVInterval(currentContig, currentStart, currentEnd), values);
                    currentStart = newInterval.getStart();
                    currentEnd = newInterval.getEnd();
                    values = new ArrayList<>(next.getValue());
                }
            }

        }
        if (currentStart != -1) {
            mergedTree.put(new SVInterval(currentContig, currentStart, currentEnd), values);
        }

        return mergedTree;
    }

    private static boolean overlaps(final SVInterval newInterval, final int currentEnd, final int clusterSize) {
        return currentEnd + clusterSize > newInterval.getStart() - clusterSize;
    }

    static String intervalTreeToBedRecord(final String barcode, final SVIntervalTree.Entry<List<ReadInfo>> node, final String[] contigNames) {
        final StringBuilder builder = new StringBuilder();
        builder.append(contigNames[node.getInterval().getContig()]);
        builder.append("\t");
        builder.append(node.getInterval().getStart());
        builder.append("\t");
        builder.append(node.getInterval().getEnd());
        builder.append("\t");
        builder.append(barcode);
        builder.append("\t");
        builder.append(node.getValue() == null ? "." : node.getValue().size());
        builder.append("\t");
        builder.append("+");
        builder.append("\t");
        builder.append(node.getInterval().getStart());
        builder.append("\t");
        builder.append(node.getInterval().getEnd());
        builder.append("\t");
        builder.append("0,0,255");
        builder.append("\t");
        builder.append(node.getValue() == null ? "." : node.getValue().size());
        final List<ReadInfo> reads;
        if (node.getValue() != null) {
            reads = node.getValue();
            reads.sort(Comparator.comparingInt(ReadInfo::getStart));
        } else {
            reads = null;
        }
        builder.append("\t");
        builder.append(reads == null ? "." : reads.stream().map(r -> String.valueOf(r.getEnd() - r.getStart() + 1)).collect(Collectors.joining(",")));
        builder.append("\t");
        builder.append(reads == null ? "." : reads.stream().map(r -> String.valueOf(r.getStart() - node.getInterval().getStart())).collect(Collectors.joining(",")));
        builder.append("\t");
        builder.append(reads == null ? "." : reads.stream().mapToInt(ReadInfo::getMapq).max().orElse(-1));
        return builder.toString();
    }


    /**
     * A lightweight object to summarize reads for the purposes of collecting linked read information
     */
    @DefaultSerializer(ReadInfo.Serializer.class)
    static class ReadInfo {
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
            public void write( final Kryo kryo, final Output output, final ReadInfo interval ) {
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


}

