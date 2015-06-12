package org.broadinstitute.hellbender.tools.walkers.bqsr;

import org.broadinstitute.hellbender.CommandLineProgramTest;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.tools.IntegrationTestSpec;
import org.broadinstitute.hellbender.utils.test.ArgumentsBuilder;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class ApplyBQSRDataflowIntegrationTest extends CommandLineProgramTest {
    private static class ABQSRTest {
        final String bam;
        final String args;
        final String expectedFile;

        private ABQSRTest(String bam, String args, String expectedFile) {
            this.bam= bam;
            this.args = args;
            this.expectedFile = expectedFile;
        }

        @Override
        public String toString() {
            return String.format("ApplyBQSR(args='%s')", args);
        }
    }

    final String resourceDir = getTestDataDir() + "/" + "BQSR" + "/";
    final String hiSeqBam = resourceDir + "HiSeq.1mb.1RG.2k_lines.alternate_allaligned.bam";

    @DataProvider(name = "ApplyBQSRTest")
    public Object[][] createABQSRTestData() {
        List<Object[]> tests = new ArrayList<>();

        tests.add(new Object[]{new ABQSRTest(hiSeqBam, "", resourceDir + "expected.HiSeq.1mb.1RG.2k_lines.bqsr.alternate_allaligned.bam")});
        tests.add(new Object[]{new ABQSRTest(hiSeqBam, " -qq -1", resourceDir + "expected.HiSeq.1mb.1RG.2k_lines.bqsr.qq-1.alternate_allaligned.bam")});
        tests.add(new Object[]{new ABQSRTest(hiSeqBam, " -qq 6", resourceDir + "expected.HiSeq.1mb.1RG.2k_lines.bqsr.qq6.alternate_allaligned.bam")});
        tests.add(new Object[]{new ABQSRTest(hiSeqBam, " -DIQ", resourceDir + "expected.HiSeq.1mb.1RG.2k_lines.bqsr.DIQ.alternate_allaligned.bam")});

        // TODO: add test inputs with some unaligned reads

        return tests.toArray(new Object[][]{});
    }

    @Test(dataProvider = "ApplyBQSRTest")
    public void testPR(ABQSRTest params) throws IOException {
        String args =
                " -I " + params.bam +
                " --bqsr_recal_file " + resourceDir + "HiSeq.20mb.1RG.table.gz " +
                params.args +
                " -O %s";
        ArgumentsBuilder ab = new ArgumentsBuilder().add(args);
        addDataflowRunnerArgs(ab);
        IntegrationTestSpec spec = new IntegrationTestSpec(
                ab.getString(),
                Arrays.asList(params.expectedFile));
        spec.executeTest("testPrintReads-" + params.args, this);
    }

    @Test
    public void testPRNoFailWithHighMaxCycle() throws IOException {
        String args = " -I " + hiSeqBam +
                " --bqsr_recal_file " + resourceDir + "HiSeq.1mb.1RG.highMaxCycle.table.gz" +
                " -O /dev/null";
        ArgumentsBuilder ab = new ArgumentsBuilder().add(args);
        addDataflowRunnerArgs(ab);
        IntegrationTestSpec spec = new IntegrationTestSpec(
                ab.getString() ,
                Arrays.<String>asList());
        spec.executeTest("testPRNoFailWithHighMaxCycle", this);      //this just checks that the tool does not blow up
    }

    @Test
    public void testPRFailWithLowMaxCycle() throws IOException {
        String args =  " -I " + hiSeqBam +
                " --bqsr_recal_file " + resourceDir + "HiSeq.1mb.1RG.lowMaxCycle.table.gz" +
                " -O /dev/null";
        ArgumentsBuilder ab = new ArgumentsBuilder().add(args);
        addDataflowRunnerArgs(ab);
        IntegrationTestSpec spec = new IntegrationTestSpec(
                       ab.getString(),
                0,
                UserException.class);
        spec.executeTest("testPRFailWithLowMaxCycle", this);
    }

}
