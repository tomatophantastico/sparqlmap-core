package org.aksw.sparqlmap.core.r2rml;

import java.util.List;
import java.util.Set;

import org.aksw.sparqlmap.core.r2rml.QuadMap.LogicalTable;

import jersey.repackaged.com.google.common.collect.Lists;
import jersey.repackaged.com.google.common.collect.Sets;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.vocabulary.RDF;

public class QuadMapLoader {

  public static Set<QuadMap> load(Model model) {
    Set<QuadMap> quadMaps = Sets.newHashSet();

    for (Resource triplesMapUri : model.listResourcesWithProperty(RDF.type, R2RML.TRIPLESMAP).toList()) {
      
      
      
      Resource logicalTable = triplesMapUri.getRequiredProperty(R2RML.HASLOGICALTABLE).getObject().asResource();
      StmtIterator tablenames = model.listStatements(logicalTable, R2RML.HASTABLENAME, (RDFNode) null);
      String tablename = LoaderHelper.getSingleLiteralObjectValue(tablenames);
      tablename = R2RMLHelper.unescape(tablename);
      StmtIterator queries = model.listStatements(logicalTable, R2RML.HASSQLQUERY, (RDFNode) null);
      String query = LoaderHelper.getSingleLiteralObjectValue(queries);
      query = R2RMLHelper.cleanSelectQuery(query);
      StmtIterator versions = model.listStatements(logicalTable, R2RML.SQLVERSION, (RDFNode) null);
      Resource versionResource = LoaderHelper.getSingleResourceObject(versions);
      String version  = versionResource != null?versionResource.getURI():null;
      // load the subject Map

      StmtIterator subjectMaps = model.listStatements(triplesMapUri, R2RML.HASSUBJECTMAP, (RDFNode) null);
      Resource subjectMap = LoaderHelper.getSingleResourceObject(subjectMaps);
      TermMap subject = TermMapLoader.load(model, subjectMap);

      // here we store the graph maps declared on the subject, and therefore
      // triples map level.
      List<TermMap> triplesMapGraphMaps = Lists.newArrayList();

      // and load the respective term maps
      List<Statement> graphMaps = subjectMap.listProperties(R2RML.HASGRAPHMAP).toList();
      for (Statement graphMapStmnt : graphMaps) {
        triplesMapGraphMaps.add(TermMapLoader.load(model, graphMapStmnt.getObject().asResource()));
      }

      // get the pos
      List<Statement> pos = model.listStatements(triplesMapUri, R2RML.HASPREDICATEOBJECTMAP, (RDFNode) null).toList();

      for (Statement po : pos) {
        if (!po.getObject().isResource()) {
          throw new R2RMLValidationException("non-resource in object position of rr:predicateObjectMap");
        }

        Resource poMap = po.getObject().asResource();

        Resource predicateMap = LoaderHelper.getSingleResourceObject(
            model.listStatements(poMap, R2RML.HASPREDICATEMAP, (RDFNode) null));
        TermMap predicate = TermMapLoader.load(model, predicateMap);

        Resource objectMap = LoaderHelper.getSingleResourceObject(
             model.listStatements(poMap, R2RML.HASOBJECTMAP, (RDFNode) null));
        TermMap object = TermMapLoader.load(model, objectMap);

        List<TermMap> pographs = Lists.newArrayList();
        // and load the respective term maps
        List<Statement> pographMaps = poMap.listProperties(R2RML.HASGRAPHMAP).toList();
        for (Statement pographMapStmnt : pographMaps) {
          triplesMapGraphMaps.add(TermMapLoader.load(model, pographMapStmnt.getObject().asResource()));
        }

        // collect all the graph information
        Set<TermMap> graphs = Sets.newHashSet();
        graphs.addAll(triplesMapGraphMaps);
        graphs.addAll(pographs);

        if (graphs.isEmpty()) {
          graphs.add(TermMapLoader.defaultGraphTermMap());
        }
        // here we got everything we need, so we construct the quadmaps
        
        
        for(TermMap graph : graphs){
          QuadMap quadMap = new QuadMap();
          quadMap.setTriplesMapUri(triplesMapUri.getURI());
          quadMap.setGraph(graph);
          quadMap.setSubject(subject);
          quadMap.setPredicate(predicate);
          quadMap.setObject(object);
          
          LogicalTable ltab = new LogicalTable();
          ltab.setTablename(tablename);
          ltab.setQuery(query);
          ltab.setVersion(version);
          quadMap.setLogicalTable(ltab);
          quadMaps.add(quadMap);
        }

      }

    }
    
    return quadMaps;

  }

}
