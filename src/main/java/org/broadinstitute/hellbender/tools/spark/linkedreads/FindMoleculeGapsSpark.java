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
import org.broadinstitute.hellbender.exceptions.GATKException;
import org.broadinstitute.hellbender.tools.spark.sv.evidence.ReadMetadata;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVInterval;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVIntervalTree;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVUtils;
import org.broadinstitute.hellbender.tools.spark.utils.IntHistogram;
import scala.Tuple2;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@CommandLineProgramProperties(
        summary = "Find interesting gaps in molecule coverage",
        oneLineSummary = "FindMoleculeGapsSpark on Spark",
        programGroup = LinkedReadsProgramGroup.class
)
public class FindMoleculeGapsSpark extends GATKSparkTool {
    private static final long serialVersionUID = 1L;
    public static final int MAX_TRACKED_VALUE = 10000;

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
                    for (int j = 0; j < numObservations(gap); j++) {
                        intHistogram.addObservation(gap);
                    }
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

        logger.info("Median gap size: " + fullIntHistogram.getCDF().median());

        Broadcast<IntHistogram> broadcastHistogram = ctx.broadcast(fullIntHistogram);

        final JavaPairRDD<SVInterval, IntHistogram> gapHistogramsAtQueryPoints = barcodeIntervals.flatMapToPair(p -> {
            final Tuple2<SVInterval, List<ReadInfo>> moleculeInfo = p._2();
            final List<Tuple2<SVInterval, Integer>> gaps = getGaps(moleculeInfo);
            return gaps.iterator();
        }).aggregateByKey(null,
                (hist, gap) -> {
                    if (hist == null) hist = new IntHistogram(MAX_TRACKED_VALUE);
                    hist.addObservation(gap);
                    return hist;
                },
                (hist1, hist2) -> {
                    if (hist1 == null) hist1 = new IntHistogram(MAX_TRACKED_VALUE);
                    if (hist2 == null) hist2 = new IntHistogram(MAX_TRACKED_VALUE);
                    hist1.addObservations(hist2);
                    return hist1;
                });

         gapHistogramsAtQueryPoints.
                 filter(p -> p._1().getStart() % 25000 == 0).
                 mapValues(hist -> hist.textRep()).saveAsTextFile("foo");
    }

    private int numObservations(final int gap) {
        return gap / 100;
    }

    static List<Tuple2<SVInterval, Integer>> getGaps(final Tuple2<SVInterval, List<ReadInfo>> moleculeInfo) {
        final SVInterval moleculeInterval = moleculeInfo._1();
        final int contig = moleculeInterval.getContig();
        final List<ReadInfo> readInfos = moleculeInfo._2();
        int readIndex = 0;
        final int firstReadStart = readInfos.get(0).getStart();
        final int lastReadStart = readInfos.get(readInfos.size() - 1).getStart();
        final List<Tuple2<SVInterval, Integer>> gaps = new ArrayList<>(readInfos.size());
        for (int i = firstReadStart + (100 - firstReadStart % 100); i <= lastReadStart; i = i + 100) {
            while (readInfos.get(readIndex).getStart() < i) readIndex++;
            final int gap = readInfos.get(readIndex).getStart() - readInfos.get(readIndex - 1).getStart();
            gaps.add(new Tuple2<>(new SVInterval(contig, i, i), gap));
        }
        return gaps;
    }

    private JavaPairRDD<String, Tuple2<SVInterval, List<ReadInfo>>> parseBarcodeIntervals(final JavaSparkContext ctx,
                                                                                          final Broadcast<Map<String, Integer>> broadcastContigNameMap,
                                                                                          final File inputLinkedReads) {
//        final List<Tuple2<String, Tuple2<SVInterval, List<ReadInfo>>>> results = new ArrayList<>(3000000);
//        try(final BlockCompressedInputStream inputStream = new BlockCompressedInputStream(new FileInputStream(inputLinkedReads))) {
//            String line = null;
//            while ((line = inputStream.readLine()) != null) {
//                final Tuple2<String, Tuple2<SVInterval, List<ReadInfo>>> lineValue = parseBarcodeIntervalLine(line, readMetadata);
//                results.add(lineValue);
//            }
//        } catch (FileNotFoundException e) {
//            throw new GATKException("Could not open file", e);
//        } catch (IOException e) {
//            throw new GATKException("Could not read from file", e);
//        }
//        logger.warn("Loaded " + results.size() + " records from file");
//        final JavaPairRDD<String, Tuple2<SVInterval, List<ReadInfo>>> rawRdd = ctx.parallelizePairs(results);

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
