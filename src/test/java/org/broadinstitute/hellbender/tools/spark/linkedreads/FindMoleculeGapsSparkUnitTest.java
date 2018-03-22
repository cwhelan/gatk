package org.broadinstitute.hellbender.tools.spark.linkedreads;

import org.broadinstitute.hellbender.tools.spark.sv.utils.SVInterval;
import org.testng.Assert;
import org.testng.annotations.Test;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static org.testng.Assert.*;

public class FindMoleculeGapsSparkUnitTest {

    @Test
    public void testGetGaps() {
        SVInterval moleculeInterval = new SVInterval(1, 1010, 2010);
        ArrayList<ReadInfo> readInfos = new ArrayList<>();
        readInfos.add(new ReadInfo(1, 1010, 1020, true, -1));
        readInfos.add(new ReadInfo(1, 1610, 1620, true, -1));
        readInfos.add(new ReadInfo(1, 2000, 2010, true, -1));
        Tuple2<SVInterval, List<ReadInfo>> moleculeInfo = new Tuple2<>(moleculeInterval, readInfos);
        List<Tuple2<SVInterval, Integer>> gaps = FindMoleculeGapsSpark.getGaps(moleculeInfo);

        int idx = 0;
        for (int i = 1100; i <= 1600; i = i + 100) {
            Assert.assertEquals(gaps.get(idx), new Tuple2<>(new SVInterval(1, i, i), 600));
            idx = idx + 1;
        }
        for (int i = 1700; i <= 2000; i = i + 100) {
            Assert.assertEquals(gaps.get(idx), new Tuple2<>(new SVInterval(1, i, i), 390));
            idx = idx + 1;
        }
        Assert.assertEquals( gaps.size(), 10);
    }

    @Test
    public void testGetGapsEvenStart() {
        SVInterval moleculeInterval = new SVInterval(1, 1000, 2010);
        ArrayList<ReadInfo> readInfos = new ArrayList<>();
        readInfos.add(new ReadInfo(1, 1000, 1010, true, -1));
        readInfos.add(new ReadInfo(1, 1610, 1620, true, -1));
        readInfos.add(new ReadInfo(1, 2000, 2010, true, -1));
        Tuple2<SVInterval, List<ReadInfo>> moleculeInfo = new Tuple2<>(moleculeInterval, readInfos);
        List<Tuple2<SVInterval, Integer>> gaps = FindMoleculeGapsSpark.getGaps(moleculeInfo);

        int idx = 0;
        for (int i = 1000; i <= 1600; i = i + 100) {
            Assert.assertEquals(gaps.get(idx), new Tuple2<>(new SVInterval(1, i, i), 610));
            idx = idx + 1;
        }
        for (int i = 1700; i <= 2000; i = i + 100) {
            Assert.assertEquals(gaps.get(idx), new Tuple2<>(new SVInterval(1, i, i), 390));
            idx = idx + 1;
        }
        Assert.assertEquals( gaps.size(), 11);
    }

    @Test
    public void testGapsMissingReadAtStart() {
        SVInterval moleculeInterval = new SVInterval(1, 1000, 2010);
        ArrayList<ReadInfo> readInfos = new ArrayList<>();
        readInfos.add(new ReadInfo(1, 1610, 1620, true, -1));
        readInfos.add(new ReadInfo(1, 2000, 2010, true, -1));
        Tuple2<SVInterval, List<ReadInfo>> moleculeInfo = new Tuple2<>(moleculeInterval, readInfos);
        List<Tuple2<SVInterval, Integer>> gaps = FindMoleculeGapsSpark.getGaps(moleculeInfo);

        int idx = 0;
        for (int i = 1700; i <= 2000; i = i + 100) {
            Assert.assertEquals(gaps.get(idx), new Tuple2<>(new SVInterval(1, i, i), 390));
            idx = idx + 1;
        }
        Assert.assertEquals( gaps.size(), 4);
    }

}