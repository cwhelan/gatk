package org.broadinstitute.hellbender.cmdline.programgroups;

import org.broadinstitute.barclay.argparser.CommandLineProgramGroup;
import org.broadinstitute.hellbender.utils.help.HelpConstants;

public class LinkedReadsProgramGroup implements CommandLineProgramGroup {

    @Override
    public String getName() { return HelpConstants.DOC_CAT_LINKED_READ; }

    @Override
    public String getDescription() { return HelpConstants.DOC_CAT_LINKED_READ_SUMMARY; }

}
