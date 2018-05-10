package org.broadinstitute.hellbender.tools.spark.linkedreads;

import org.broadinstitute.hellbender.GATKBaseTest;
import org.broadinstitute.hellbender.tools.spark.sv.utils.PairedStrandedIntervals;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVInterval;
import org.broadinstitute.hellbender.tools.spark.sv.utils.SVIntervalTree;
import org.broadinstitute.hellbender.tools.spark.sv.utils.StrandedInterval;
import org.testng.Assert;
import org.testng.annotations.Test;
import scala.Tuple2;

import java.util.*;

public class FindLinkedReadEvidenceLinksTest extends GATKBaseTest {

    @Test
    public void testFindLinksWithEnoughOverlappers() throws Exception {

        final List<Tuple2<SVInterval, Integer>> moleculeList = new ArrayList<>();
        moleculeList.add(new Tuple2<>(new SVInterval(0, 10000, 12000), 1));
        moleculeList.add(new Tuple2<>(new SVInterval(0, 50100, 51000), 1));

        moleculeList.add(new Tuple2<>(new SVInterval(0, 10400, 21500), 2));
        moleculeList.add(new Tuple2<>(new SVInterval(0, 50800, 55600), 2));

        moleculeList.sort(Comparator.comparing(p -> p._1));

        final Map<Integer, SVIntervalTree<Boolean>> intervalEnds = new HashMap<>();
        final SVIntervalTree<Boolean> molecule1EndTree = new SVIntervalTree<>();
        molecule1EndTree.put(new SVInterval(0, 9000, 10000), false);
        molecule1EndTree.put(new SVInterval(0, 12000, 13000), true);
        molecule1EndTree.put(new SVInterval(0, 49100, 50100), false);
        molecule1EndTree.put(new SVInterval(0, 51000, 52000), true);

        intervalEnds.put(1, molecule1EndTree);

        final SVIntervalTree<Boolean> molecule2EndTree = new SVIntervalTree<>();
        molecule2EndTree.put(new SVInterval(0, 9400, 10400), false);
        molecule2EndTree.put(new SVInterval(0, 21500, 22500), true);
        molecule2EndTree.put(new SVInterval(0, 49800, 50800), false);
        molecule2EndTree.put(new SVInterval(0, 55600, 56600), true);
        intervalEnds.put(2, molecule2EndTree);

        final Iterator<Tuple2<PairedStrandedIntervals, FindLinkedReadEvidenceLinks.ClusteredLinkInfo>> linksWithEnoughOverlappers =
                FindLinkedReadEvidenceLinks.findLinksWithEnoughOverlappers(1000, moleculeList.iterator(),
                        intervalEnds, new String[] { "A", "B", "C", "D" }, 2);

        List<Tuple2<PairedStrandedIntervals, FindLinkedReadEvidenceLinks.ClusteredLinkInfo>> expectedLinks = new ArrayList<>();
        expectedLinks.add(
                new Tuple2<>(
                        new PairedStrandedIntervals(
                                new StrandedInterval(new SVInterval(0, 9400, 10000), false),
                                new StrandedInterval(new SVInterval( 0, 49800, 50100), false)),
                        new FindLinkedReadEvidenceLinks.ClusteredLinkInfo(
                            new HashSet<>(Arrays.asList(1, 2))
                        )
                ));

        int actualLinks = 0;
        while (linksWithEnoughOverlappers.hasNext()) {
            Tuple2<PairedStrandedIntervals, FindLinkedReadEvidenceLinks.ClusteredLinkInfo> link = linksWithEnoughOverlappers.next();
            Assert.assertEquals(link, expectedLinks.get(actualLinks));
            actualLinks++;
        }
        Assert.assertEquals(actualLinks, 1);
    }
}