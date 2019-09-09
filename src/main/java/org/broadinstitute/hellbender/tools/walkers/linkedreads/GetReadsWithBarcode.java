package org.broadinstitute.hellbender.tools.walkers.linkedreads;

import org.broadinstitute.barclay.argparser.Argument;
import org.broadinstitute.barclay.argparser.CommandLineProgramProperties;
import org.broadinstitute.barclay.help.DocumentedFeature;
import org.broadinstitute.hellbender.cmdline.StandardArgumentDefinitions;
import org.broadinstitute.hellbender.cmdline.programgroups.LinkedReadsProgramGroup;
import org.broadinstitute.hellbender.engine.FeatureContext;
import org.broadinstitute.hellbender.engine.GATKPathSpecifier;
import org.broadinstitute.hellbender.engine.ReadWalker;
import org.broadinstitute.hellbender.engine.ReferenceContext;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.io.IOUtils;
import org.broadinstitute.hellbender.utils.read.GATKRead;
import org.broadinstitute.hellbender.utils.read.SAMFileGATKReadWriter;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

@CommandLineProgramProperties(
        summary = "Prints reads from the input SAM/BAM/CRAM which match the given list of barcodes to the SAM/BAM/CRAM file.",
        oneLineSummary = "Print reads that match barcodes in the SAM/BAM/CRAM file",
        programGroup = LinkedReadsProgramGroup.class
)
@DocumentedFeature
public final class GetReadsWithBarcode extends ReadWalker {

    @Argument(fullName = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            doc="Write output to this file")
    public String output;
    private SAMFileGATKReadWriter outputWriter;

    @Argument(fullName = "barcode-list",
              shortName = "B",
              doc="List of barcodes to select")
    public GATKPathSpecifier barcodeListFile;

    final Set<String> barcodes = new HashSet<>();

    @Override
    public void onTraversalStart() {
        String line;
        try (final BufferedReader barcodeReader = new BufferedReader(IOUtils.makeReaderMaybeGzipped(barcodeListFile.toPath()))) {
            while ((line = barcodeReader.readLine()) != null && !line.isEmpty()) {
                barcodes.add(line);
            }
        } catch (IOException e) {
            throw new UserException("Can't read barcode list file " + barcodeListFile, e);
        }

        outputWriter = createSAMWriter(IOUtils.getPath(output), true);
    }

    @Override
    public void apply(GATKRead read, ReferenceContext referenceContext, FeatureContext featureContext ) {
        if (read.hasAttribute("BX") && barcodes.contains(read.getAttributeAsString("BX"))) {
            outputWriter.addRead(read);
        }
    }

    @Override
    public void closeTool() {
        if ( outputWriter != null ) {
            outputWriter.close();
        }
    }
}
