package org.broadinstitute.hellbender.tools.spark.linkedreads;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import htsjdk.samtools.SAMFileHeader;
import org.broadinstitute.hellbender.tools.spark.linkedreads.ExtractLinkedReadsSpark.ReadInfo;
import org.broadinstitute.hellbender.tools.spark.sv.evidence.ReadMetadata;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVInterval;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVIntervalTree;
import org.broadinstitute.hellbender.utils.read.ArtificialReadUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.testng.Assert;
import org.testng.annotations.Test;
import scala.Tuple2;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExtractLinkedReadsSparkUnitTest {

    private static final SAMFileHeader artificialSamHeader =
            ArtificialReadUtils.createArtificialSamHeaderWithGroups(2, 1, 1000000, 1);

    private static final Map<String, Integer> contigIdToContigNameMap = initContigNameMap();
    private static final String[] contigNames = new String[] {"1", "2"};

    private static Map<String, Integer> initContigNameMap() {
        final Map<String, Integer> contigIdToContigNameMap = new HashMap<>(2);
        contigIdToContigNameMap.put("1", 0);
        contigIdToContigNameMap.put("2", 1);
        return contigIdToContigNameMap;
    }

    @Test
    public void testAddReadToIntervals() {

        final SVIntervalTree<List<ReadInfo>> intervals = new SVIntervalTree<>();


        final GATKRead read1 = ArtificialReadUtils.createSamBackedRead("A", "1", 1750, 100);
        final SVIntervalTree<List<ReadInfo>> svIntervalTree = ExtractLinkedReadsSpark.addReadToIntervals(intervals,
                new ReadInfo(contigIdToContigNameMap, read1), 1000);

        final SVIntervalTree.Entry<List<ReadInfo>> node = svIntervalTree.find(new SVInterval(contigIdToContigNameMap.get("1"), 1750, 1849));
        Assert.assertNotNull(node);
        Assert.assertEquals(node.getInterval().getStart(), 1750);
        Assert.assertEquals(node.getInterval().getEnd(), 1849);
        Assert.assertEquals(node.getValue().size(), 1);

        final GATKRead read2 = ArtificialReadUtils.createSamBackedRead("B", "1", 1900, 100);
        final SVIntervalTree<List<ReadInfo>> svIntervalTree2 = ExtractLinkedReadsSpark.addReadToIntervals(intervals, new ReadInfo(contigIdToContigNameMap, read2), 1000);
        final SVIntervalTree.Entry<List<ReadInfo>> node2 = svIntervalTree2.find(new SVInterval(contigIdToContigNameMap.get("1"), 1750, 1999));
        Assert.assertNotNull(node2);
        Assert.assertEquals(node2.getInterval().getStart(), 1750);
        Assert.assertEquals(node2.getInterval().getEnd(), 1999);
        Assert.assertEquals(node2.getValue().size(), 2);

        final GATKRead read3 = ArtificialReadUtils.createSamBackedRead("D", "1", 3500, 100);
        final SVIntervalTree<List<ReadInfo>> stringSVIntervalTree3 = ExtractLinkedReadsSpark.addReadToIntervals(intervals, new ReadInfo(contigIdToContigNameMap, read3), 1000);
        final SVIntervalTree.Entry<List<ReadInfo>> node3 = stringSVIntervalTree3.find(new SVInterval(contigIdToContigNameMap.get("1"), 1750, 1999));
        Assert.assertNotNull(node3);
        Assert.assertEquals(node3.getInterval().getStart(), 1750);
        Assert.assertEquals(node3.getInterval().getEnd(), 1999);
        Assert.assertEquals(node3.getValue().size(), 2);

        final SVIntervalTree.Entry<List<ReadInfo>> node4 = stringSVIntervalTree3.find(new SVInterval(contigIdToContigNameMap.get("1"), 3500, 3599));
        Assert.assertNotNull(node4);
        Assert.assertEquals(node4.getInterval().getStart(), 3500);
        Assert.assertEquals(node4.getInterval().getEnd(), 3599);
        Assert.assertEquals(node4.getValue().size(), 1);

        final GATKRead read4 = ArtificialReadUtils.createSamBackedRead("C", "2", 500, 100);
        final SVIntervalTree<List<ReadInfo>> svIntervalTree4 = ExtractLinkedReadsSpark.addReadToIntervals(intervals, new ReadInfo(contigIdToContigNameMap, read4), 2500);
        final SVIntervalTree.Entry<List<ReadInfo>> node5 = svIntervalTree4.find(new SVInterval(contigIdToContigNameMap.get("1"), 1750, 1999));
        Assert.assertNotNull(node5);
        Assert.assertEquals(node5.getValue().size(), 2);

        final SVIntervalTree.Entry<List<ReadInfo>> node6 = svIntervalTree4.find(new SVInterval(contigIdToContigNameMap.get("2"), 500, 599));
        Assert.assertNotNull(node6);
        Assert.assertEquals(node6.getValue().size(), 1);

    }

    @Test
    public void testMergeSVIntervalTrees() {
        final SAMFileHeader artificialSamHeader = ArtificialReadUtils.createArtificialSamHeader(1, 1, 10000);
        final SVIntervalTree<List<ReadInfo>> tree1 = createTestSVIntervalTree1(artificialSamHeader);
        final SVIntervalTree<List<ReadInfo>> tree2 = new SVIntervalTree<>();

        final List<ReadInfo> resultList2 = new ArrayList<>();
        resultList2.add(new ReadInfo(contigIdToContigNameMap, ArtificialReadUtils.createArtificialRead(artificialSamHeader, "1", 0, 3000, 100)));
        resultList2.add(new ReadInfo(contigIdToContigNameMap, ArtificialReadUtils.createArtificialRead(artificialSamHeader, "1", 0, 3004, 100)));
        resultList2.add(new ReadInfo(contigIdToContigNameMap, ArtificialReadUtils.createArtificialRead(artificialSamHeader, "1", 0, 3010, 100)));
        tree1.put(new SVInterval(contigIdToContigNameMap.get("1"), 3000, 3100), resultList2);

        final List<ReadInfo> resultList3 = new ArrayList<>();
        resultList3.add(new ReadInfo(contigIdToContigNameMap, ArtificialReadUtils.createArtificialRead(artificialSamHeader, "1", 0, 1500, 100)));
        resultList3.add(new ReadInfo(contigIdToContigNameMap, ArtificialReadUtils.createArtificialRead(artificialSamHeader, "1", 0, 1504, 100)));
        resultList3.add(new ReadInfo(contigIdToContigNameMap, ArtificialReadUtils.createArtificialRead(artificialSamHeader, "1", 0, 1510, 100)));
        resultList3.add(new ReadInfo(contigIdToContigNameMap, ArtificialReadUtils.createArtificialRead(artificialSamHeader, "1", 0, 1520, 100)));
        resultList3.add(new ReadInfo(contigIdToContigNameMap, ArtificialReadUtils.createArtificialRead(artificialSamHeader, "1", 0, 1524, 100)));
        resultList3.add(new ReadInfo(contigIdToContigNameMap, ArtificialReadUtils.createArtificialRead(artificialSamHeader, "1", 0, 1530, 100)));
        resultList3.add(new ReadInfo(contigIdToContigNameMap, ArtificialReadUtils.createArtificialRead(artificialSamHeader, "1", 0, 1540, 100)));
        resultList3.add(new ReadInfo(contigIdToContigNameMap, ArtificialReadUtils.createArtificialRead(artificialSamHeader, "1", 0, 1544, 100)));
        resultList3.add(new ReadInfo(contigIdToContigNameMap, ArtificialReadUtils.createArtificialRead(artificialSamHeader, "1", 0, 1550, 100)));
        tree2.put(new SVInterval(contigIdToContigNameMap.get("1"), 1500, 1649), resultList3);
        final List<ReadInfo> resultList4 = new ArrayList<>();
        resultList4.add(new ReadInfo(contigIdToContigNameMap, ArtificialReadUtils.createArtificialRead(artificialSamHeader, "1", 0, 3300, 200)));
        tree2.put(new SVInterval(contigIdToContigNameMap.get("1"), 3300, 3500), resultList4);

        final SVIntervalTree<List<ReadInfo>> mergedTree = ExtractLinkedReadsSpark.mergeIntervalTrees(tree1, tree2, 250);
        Assert.assertEquals(mergedTree.size(), 2);
        final SVIntervalTree.Entry<List<ReadInfo>> mergedNode1 = mergedTree.find(new SVInterval(contigIdToContigNameMap.get("1"), 1000, 1649));
        Assert.assertNotNull(mergedNode1);
        Assert.assertEquals(mergedNode1.getInterval().getStart(), 1000);
        Assert.assertEquals(mergedNode1.getInterval().getEnd(), 1649);
        Assert.assertEquals(mergedNode1.getValue().size(), 14);

        final SVIntervalTree.Entry<List<ReadInfo>> mergedNode2 = mergedTree.find(new SVInterval(contigIdToContigNameMap.get("1"), 3000, 3500));
        Assert.assertNotNull(mergedNode2);
        Assert.assertEquals(mergedNode2.getInterval().getStart(), 3000);
        Assert.assertEquals(mergedNode2.getInterval().getEnd(), 3500);

        Assert.assertEquals(mergedNode2.getValue().size(), 4);

    }

    @Test
    public void testSVIntervalTreeToBed() {
        final SAMFileHeader artificialSamHeader = ArtificialReadUtils.createArtificialSamHeader(1, 1, 10000);

        final SVIntervalTree<List<ReadInfo>> tree = createTestSVIntervalTree1(artificialSamHeader);

        final String barcode = "ACTGACTG";

        final String bedRecord = ExtractLinkedReadsSpark.intervalTreeToBedRecord(barcode, tree.iterator().next(), contigNames);
        Assert.assertEquals(bedRecord, "1\t1000\t1160\tACTGACTG\t5\t+\t1000\t1160\t0,0,255\t5\t10,11,10,9,10\t0,20,25,45,150\t0");

    }

    private SVIntervalTree<List<ReadInfo>> createTestSVIntervalTree1(final SAMFileHeader artificialSamHeader) {
        final SVIntervalTree<List<ReadInfo>> tree = new SVIntervalTree<>();
        final List<ReadInfo> resultList1 = new ArrayList<>();
        resultList1.add(new ReadInfo(contigIdToContigNameMap, ArtificialReadUtils.createArtificialRead(artificialSamHeader, "1", 0, 1000, "ACACACACAC".getBytes(), "2222222222".getBytes(), "10M")));
        resultList1.add(new ReadInfo(contigIdToContigNameMap, ArtificialReadUtils.createArtificialRead(artificialSamHeader, "1", 0, 1020, "GTGTGTGTGT".getBytes(), "3333333333".getBytes(), "6M1D4M")));
        resultList1.add(new ReadInfo(contigIdToContigNameMap, ArtificialReadUtils.createArtificialRead(artificialSamHeader, "1", 0, 1025, "AGAGAGAGAG".getBytes(), "4444444444".getBytes(), "10M")));
        resultList1.add(new ReadInfo(contigIdToContigNameMap, ArtificialReadUtils.createArtificialRead(artificialSamHeader, "1", 0, 1045, "GCGCGCGCGC".getBytes(), "5555555555".getBytes(), "5M1I4M")));
        resultList1.add(new ReadInfo(contigIdToContigNameMap, ArtificialReadUtils.createArtificialRead(artificialSamHeader, "1", 0, 1150, "TTTTTTTTTT".getBytes(), "6666666666".getBytes(), "10M")));
        tree.put(new SVInterval(contigIdToContigNameMap.get("1"), 1000, 1160), resultList1);
        return tree;
    }

    @Test
    public void testSerializeTree() {

        final SVIntervalTree<List<ReadInfo>> tree = createTestSVIntervalTree1(artificialSamHeader);
        final ByteArrayOutputStream bos = new ByteArrayOutputStream();
        final Output out = new Output(bos);
        final Kryo kryo = new Kryo();
        kryo.writeClassAndObject(out, tree);
        out.flush();

        final ByteArrayInputStream bis = new ByteArrayInputStream(bos.toByteArray());
        final Input in = new Input(bis);
        @SuppressWarnings("unchecked")
        final SVIntervalTree<List<ReadInfo>> tree2 = (SVIntervalTree<List<ReadInfo>>)kryo.readClassAndObject(in);
        Assert.assertEquals(tree.size(), tree2.size());
        final SVIntervalTree.Entry<List<ReadInfo>> listEntry = tree2.find(new SVInterval(contigIdToContigNameMap.get("1"), 1000, 1160));
        Assert.assertTrue(listEntry != null);
        Assert.assertEquals(listEntry.getValue().size(), 5);
    }

    @Test
    public void testCleanOverlappingTreeEntries() {
        final SVIntervalTree<List<ReadInfo>> tree = new SVIntervalTree<>();
        final List<ReadInfo> reads = new ArrayList<>();
        reads.add(new ReadInfo(1, 100, 200, true, 50));
        reads.add(new ReadInfo(1, 150, 250, true, 55));
        reads.add(new ReadInfo(1, 175, 275, true, 60));
        tree.put(new SVInterval(1, 100, 275), reads);
        final SVIntervalTree<List<ReadInfo>> cleanedTree = ExtractLinkedReadsSpark.cleanOverlappingTreeEntries(tree);
        Assert.assertEquals(1, cleanedTree.size());
        final SVIntervalTree.Entry<List<ReadInfo>> listEntry = cleanedTree.find(new SVInterval(1, 100, 275));
        Assert.assertNotNull(listEntry);
        Assert.assertEquals(listEntry.getValue().size(), 1);
        Assert.assertEquals(listEntry.getValue().size(), 1);
        Assert.assertEquals(listEntry.getValue().get(0), new ReadInfo(1, 175, 275, true, 60));
    }

}