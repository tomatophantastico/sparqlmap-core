package org.aksw.sparqlmap.core.translate.metamodel;

import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.aksw.sparqlmap.core.ContextConfiguration;
import org.aksw.sparqlmap.core.Dumper;
import org.aksw.sparqlmap.core.r2rml.QuadMap;
import org.aksw.sparqlmap.core.r2rml.QuadMap.LogicalTable;
import org.aksw.sparqlmap.core.r2rml.R2RMLMapping;
import org.apache.metamodel.DataContext;
import org.elasticsearch.common.collect.Lists;

import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.riot.writer.NQuadsWriter;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Quad;

public class DumperMetaModel implements Dumper{


  private R2RMLMapping r2rmlMapping;
  
  private MetaModelContext mcontext;
 
  
  
  
  
  
  public DumperMetaModel(MetaModelContext mcontext, R2RMLMapping r2rmlMapping) {
    super();
    
    
    this.mcontext = mcontext;
    this.r2rmlMapping = r2rmlMapping;
  }



  public DatasetGraph dumpDatasetGraph(){
    
    return MetaModelQueryDump.assembleDs(r2rmlMapping.getQuadMaps().values(), mcontext.getDataContext());
    
  }



  @Override
  public Iterator<Quad> streamDump() {
    
    
    return dumpDatasetGraph().find();
    
//    List<Iterator<Quad>> quadIters = Lists.newArrayList();
//    
//    
//    for(String mappingUri : Sets.newTreeSet(r2rmlMapping.getQuadMaps().keys())){
//      
//      List<QuadMap> quadMaps  = Lists.newArrayList(r2rmlMapping.getQuadMaps().get(mappingUri));
//      //get the Logical table from the first quad map, they *must* be all the same by R2RML convention
//      LogicalTable ltable = quadMaps.get(0).getLogicalTable();
//      //get all columns mentioned in the quads
//      
//      
//      MetaModelQueryWrapper qw = new MetaModelQueryWrapper(mcontext);
//       
//      int quadcount = 0; 
//       
//       for(QuadMap quadMap: quadMaps){
//         Quad dumpQuad = new Quad(NodeFactory.createVariable("g_" + quadcount ), 
//               NodeFactory.createVariable("s"), NodeFactory.createVariable("p_"+quadcount), NodeFactory.createVariable("o_"+quadcount));
//         quadcount++;
//         
//         qw.addQuad(dumpQuad, quadMap, true);
//
//        }
//       
//      
//       quadIters.add(new QueryExecutor(qw, mcontext.getConConf().getBaseUri(),mcontext.getDataContext()));
//
//    }
//    return     Iterators.concat(quadIters.iterator());
  }



  @Override
  public void streamDump(OutputStream stream) {

    NQuadsWriter.write(stream, streamDump());
    
  }
  
  
  
  
  
 
  
   
  

}