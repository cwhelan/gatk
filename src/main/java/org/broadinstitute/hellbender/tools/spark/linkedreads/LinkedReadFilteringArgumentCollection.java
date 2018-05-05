package org.broadinstitute.hellbender.tools.spark.linkedreads;

import org.broadinstitute.barclay.argparser.Argument;

import java.io.Serializable;

public class LinkedReadFilteringArgumentCollection implements Serializable {
    private static final long serialVersionUID = 1L;

    @Argument(fullName = "min-read-count-per-molecule", shortName = "min-read-count-per-molecule", doc="Minimum number of reads to call a molecule", optional=true)
    public int minReadCountPerMol = 2;

    @Argument(fullName = "edge-read-mapq-threshold", shortName = "edge-read-mapq-threshold", doc="Mapq below which to trim reads from starts and ends of molecules", optional=true)
    public int edgeReadMapqThreshold = 30;

    @Argument(fullName = "min-max-mapq", shortName = "min-max-mapq", doc="Minimum highest mapq read to create a fragment", optional=true)
    public int minMaxMapq = 30;

}
