package org.broadinstitute.hellbender.tools.spark.linkedreads;

import org.broadinstitute.hellbender.tools.spark.sv.utils.SVInterval;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVIntervalTree;
import org.broadinstitute.hellbender.tools.spark.sv.utils.StrandedInterval;
import org.testng.Assert;
import org.testng.annotations.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class FindMoleculeGapsSparkUnitTest {

    @Test
    public void testGetGaps() {
        SVInterval moleculeInterval = new SVInterval(1, 1010, 2010);
        ArrayList<ReadInfo> readInfos = new ArrayList<>();
        readInfos.add(new ReadInfo(1, 1010, 1020, true, -1));
        readInfos.add(new ReadInfo(1, 2610, 2620, true, -1));
        readInfos.add(new ReadInfo(1, 2800, 2810, true, -1));
        Tuple2<SVInterval, List<ReadInfo>> moleculeInfo = new Tuple2<>(moleculeInterval, readInfos);
        List<Tuple2<StrandedInterval, Integer>> gaps = FindMoleculeGapsSpark.getInterestingGaps(moleculeInfo, 500, 1000, new SVIntervalTree<>());

        Assert.assertEquals( gaps.size(), 2);
        Assert.assertEquals( gaps.get(0)._1(), new StrandedInterval(new SVInterval(1, 1000, 2000), true));
        Assert.assertEquals( gaps.get(0)._2().intValue(), 1600);

        Assert.assertEquals( gaps.get(1)._1(), new StrandedInterval(new SVInterval(1, 2000, 3000), false));
        Assert.assertEquals( gaps.get(1)._2().intValue(), 1600);

    }


    @Test
    public void testSplitMolecule() {
        final SVIntervalTree<List<Tuple2<Boolean, Tuple2<Integer, Integer>>>> gapTree = new SVIntervalTree<>();
        final ArrayList<Tuple2<Boolean, Tuple2<Integer, Integer>>> clusters = new ArrayList<>();
        clusters.add(new Tuple2<>(true, new Tuple2<>(2000, 3000)));
        gapTree.put(new SVInterval(1, 4000, 5000), clusters);
        final ArrayList<ReadInfo> reads = new ArrayList<>();
        reads.add(new ReadInfo(1, 3500, 3600, true, 60));
        reads.add(new ReadInfo(1, 4250, 4350, true, 60));
        reads.add(new ReadInfo(1, 6750, 6850, true, 60));
        reads.add(new ReadInfo(1, 7250, 7350, true, 60));


        final List<Tuple2<String, Tuple2<SVInterval, List<ReadInfo>>>> splitMoleculesForBarcode = FindMoleculeGapsSpark.splitMoleculesForBarcode("ACTG",
                new SVInterval(1, 3500, 7350),
                reads,
                gapTree, 1000);

        Assert.assertEquals(splitMoleculesForBarcode.size(), 2);
        final Tuple2<String, Tuple2<SVInterval, List<ReadInfo>>> firstMol = splitMoleculesForBarcode.get(0);
        Assert.assertEquals(firstMol._1(), "ACTG");
        Assert.assertEquals(firstMol._2()._1(), new SVInterval(1, 3500, 4350));
        Assert.assertEquals(firstMol._2()._2().size(), 2);
        Assert.assertEquals(firstMol._2()._2().get(0), new ReadInfo(1, 3500, 3600, true, 60));
        Assert.assertEquals(firstMol._2()._2().get(1), new ReadInfo(1, 4250, 4350, true, 60));

        final Tuple2<String, Tuple2<SVInterval, List<ReadInfo>>> secondMol = splitMoleculesForBarcode.get(1);
        Assert.assertEquals(secondMol._1(), "ACTG");
        Assert.assertEquals(secondMol._2()._1(), new SVInterval(1, 6750, 7350));
        Assert.assertEquals(secondMol._2()._2().size(), 2);
        Assert.assertEquals(secondMol._2()._2().get(0), new ReadInfo(1, 6750, 6850, true, 60));
        Assert.assertEquals(secondMol._2()._2().get(1), new ReadInfo(1, 7250, 7350, true, 60));

    }

}