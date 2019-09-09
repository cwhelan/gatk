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
import org.broadinstitute.hellbender.engine.filters.ReadFilter;
import org.broadinstitute.hellbender.engine.filters.ReadFilterLibrary;
import org.broadinstitute.hellbender.exceptions.UserException;
import org.broadinstitute.hellbender.utils.read.GATKRead;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@CommandLineProgramProperties(
        summary = "Get the list of barcodes at a locus",
        oneLineSummary = "Get the list of barcodes at a locus",
        programGroup = LinkedReadsProgramGroup.class
)
@DocumentedFeature
public class GetBarcodes extends ReadWalker {

    @Argument(
            shortName = StandardArgumentDefinitions.OUTPUT_SHORT_NAME,
            fullName  = StandardArgumentDefinitions.OUTPUT_LONG_NAME,
            doc = "Output file containing barcode list.")
    public GATKPathSpecifier outputFile;

    @Override
    public boolean requiresReads() {
        return true;
    }

    @Override
    public boolean requiresIntervals() {
        return true;
    }

    final Set<String> barcodes = new HashSet<>();
    private PrintStream outputStream;


    @Override
    public void onTraversalStart() {
        final Path outPath = outputFile.toPath();
        try {
            outputStream = new PrintStream(new BufferedOutputStream(Files.newOutputStream(outPath)));
        } catch (IOException e) {
            throw new UserException("Could not open output file " + outputFile, e);
        }
        super.onTraversalStart();
    }

    @Override
    public List<ReadFilter> getDefaultReadFilters() {
        final List<ReadFilter> readFilters = new ArrayList<>(super.getDefaultReadFilters());
        readFilters.add(ReadFilterLibrary.PASSES_VENDOR_QUALITY_CHECK);
        readFilters.add(ReadFilterLibrary.MAPPED);
        readFilters.add(ReadFilterLibrary.NON_ZERO_REFERENCE_LENGTH_ALIGNMENT);
        readFilters.add(ReadFilterLibrary.NOT_DUPLICATE);
        readFilters.add(ReadFilterLibrary.NOT_SECONDARY_ALIGNMENT);
        return readFilters;
    }

    @Override
    public void apply(final GATKRead read, final ReferenceContext referenceContext, final FeatureContext featureContext) {
        final String barcode = read.getAttributeAsString("BX");
        if (barcode != null) {
            barcodes.add(barcode);
        }
    }

    @Override
    public Object onTraversalSuccess() {
        barcodes.forEach(bx -> outputStream.println(bx));
        outputStream.close();
        return null;
    }
}
