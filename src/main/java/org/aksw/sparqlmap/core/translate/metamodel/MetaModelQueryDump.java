package org.aksw.sparqlmap.core.translate.metamodel;

import java.util.Collection;
import java.util.HashSet;

import org.aksw.sparqlmap.core.TranslationContext;
import org.aksw.sparqlmap.core.r2rml.QuadMap;
import org.aksw.sparqlmap.core.r2rml.QuadMap.LogicalTable;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.GraphUtil;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.mem.GraphMem;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphMap;
import org.apache.jena.sparql.core.Quad;
import org.apache.metamodel.DataContext;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder.ListMultimapBuilder;

/**
 * ignores the joins and fetches data based on the tables of the quad maps
 * Pushes all filters as far down as possible
 * 
 * @author joerg
 *
 */
public class MetaModelQueryDump {
  
  
  

  public static DatasetGraph assembleDs(TranslationContext tcontext, DataContext context) {

    
   return assembleDs(tcontext.getQueryBinding().getBindingMap().values(), context);

  }
  
  public static DatasetGraph assembleDs(Collection<QuadMap> quadmaps, DataContext context) {
 Multimap<LogicalTable,QuadMap> bucketedMaps = bucketMaps(new HashSet<QuadMap>(quadmaps));
    
    DatasetGraphMap result = new DatasetGraphMap();

    
    ListMultimap<Node, Triple> dsbarebone = ListMultimapBuilder.hashKeys().arrayListValues().build();
    for (LogicalTable ltab : bucketedMaps.keySet()) {
      // build the query
      MetaModelSelectiveDump sd = new MetaModelSelectiveDump(ltab,bucketedMaps.get(ltab) ,context);
      dsbarebone.putAll(sd.getDump());

    }

    for (Node graphNode : dsbarebone.keySet()) {

      Graph memGraph = new GraphMem();
      GraphUtil.add(memGraph, dsbarebone.get(graphNode));

      if (graphNode.equals(Quad.defaultGraphNodeGenerated)) {
        result.setDefaultGraph(memGraph);
      } else {
        result.addGraph(graphNode, memGraph);
      }
    }

    return result;
  }
  
  
  
  public static Multimap<LogicalTable, QuadMap> bucketMaps(Collection<QuadMap> quadmaps ){
    Multimap<LogicalTable, QuadMap> bucketedMaps = HashMultimap.create();
    for (QuadMap quadmap : quadmaps) {
     bucketedMaps.put(quadmap.getLogicalTable(), quadmap);

      
    }
    return bucketedMaps;
  }

}
