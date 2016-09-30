package org.aksw.sparqlmap.core;

import java.io.OutputStream;
import java.util.Iterator;

import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.Quad;

public interface Dumper {
  
  public DatasetGraph dumpDatasetGraph();
  public Iterator<Quad> streamDump();
  public void streamDump(OutputStream stream);

}
