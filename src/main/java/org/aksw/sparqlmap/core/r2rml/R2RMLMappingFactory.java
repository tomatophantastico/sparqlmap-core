package org.aksw.sparqlmap.core.r2rml;

import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectBodyString;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;

import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.exception.R2RMLValidationException;
import org.aksw.sparqlmap.core.mapper.translate.ColumnHelper;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.aksw.sparqlmap.core.mapper.translate.FilterUtil;
import org.aksw.sparqlmap.core.r2rml.TripleMap.PO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.sparql.core.Quad;
import com.hp.hpl.jena.update.GraphStoreFactory;
import com.hp.hpl.jena.update.UpdateExecutionFactory;
import com.hp.hpl.jena.update.UpdateFactory;
import com.hp.hpl.jena.vocabulary.RDF;
import com.hp.hpl.jena.vocabulary.RDFS;

public class R2RMLMappingFactory {
  
  private static Logger log = LoggerFactory.getLogger(R2RMLMappingFactory.class);

  
  public R2RMLModel create(Model mapping, Model r2rmlSchema, DBAccess dba, DataTypeHelper dth) throws R2RMLValidationException, JSQLParserException,
  SQLException {
    
    // First examine and canonize the RDF graph
    Model reasoningModel = ModelFactory.createRDFSModel(r2rmlSchema, mapping);
    resolveRRClassStatements(reasoningModel);
    resolveR2RMLShortcuts(reasoningModel);
    resolveMultipleGraphs(reasoningModel);
    validate(reasoningModel);
    
    Multimap<String,TripleMap> tripleMaps = loadTripleMaps(reasoningModel);
    loadParentTripleStatements(reasoningModel,tripleMaps, dth);
    
    validateSchema(tripleMaps, dba);
    
    decomposeVirtualTableQueries();
    
    validatepost();
    
    
    R2RMLModel r2rmodel = new R2RMLModel(tripleMaps);
    
    
    return r2rmodel;
    
}
  
  
  private void validateSchema(Multimap<String, TripleMap> tripleMaps,
      DBAccess dba) throws R2RMLValidationException {
    for(TripleMap trm : tripleMaps.values()){
      try {
        dba.validateFromItem(trm.getFrom());
        
        for()
        
        
        
        
        
      } catch (SQLException e) {
        throw new R2RMLValidationException(
            "Error validation the logical table in mapping "
                + trm.getUri(), e);
      }
    }
    
   
    
  }


  private void resolveRRClassStatements(Model reasoningModel) {
    String query = "PREFIX  rr:   <http://www.w3.org/ns/r2rml#> "
        + "INSERT { ?tm rr:predicateObjectMap  _:newpo. "
        + "_:newpo rr:predicate <" + RDF.type.getURI() + ">."
        + "_:newpo rr:object ?class } " + "WHERE {?tm a rr:TriplesMap."
        + "?tm  rr:subjectMap ?sm." + "?sm rr:class ?class }";
    UpdateExecutionFactory.create(UpdateFactory.create(query),
        GraphStoreFactory.create(reasoningModel)).execute();
  }


  private void resolveR2RMLShortcuts(Model reasoningModel) {
    String query = "PREFIX  rr:   <http://www.w3.org/ns/r2rml#> INSERT { ?x rr:subjectMap [ rr:constant ?y ]. } WHERE {?x rr:subject ?y.}";
    UpdateExecutionFactory.create(UpdateFactory.create(query),
        GraphStoreFactory.create(reasoningModel)).execute();
    query = "PREFIX  rr:   <http://www.w3.org/ns/r2rml#> INSERT { ?x rr:predicateMap [ rr:constant ?y ]. } WHERE {?x rr:predicate ?y.}";
    UpdateExecutionFactory.create(UpdateFactory.create(query),
        GraphStoreFactory.create(reasoningModel)).execute();
    query = "PREFIX  rr:   <http://www.w3.org/ns/r2rml#> INSERT { ?x rr:objectMap [ rr:constant ?y ]. } WHERE {?x rr:object ?y.}";
    UpdateExecutionFactory.create(UpdateFactory.create(query),
        GraphStoreFactory.create(reasoningModel)).execute();
    query = "PREFIX  rr:   <http://www.w3.org/ns/r2rml#> INSERT { ?x rr:graphMap [ rr:constant ?y ]. } WHERE {?x rr:graph ?y.}";
    UpdateExecutionFactory.create(UpdateFactory.create(query),
        GraphStoreFactory.create(reasoningModel)).execute();
    reasoningModel.size();
    

  }
  
private void resolveMultipleGraphs(Model reasoningModel) {
    
    //for all triple maps with multiple graph statements we first get all the subject triple maps and put them into the po maps
    
    List<Resource> allTripleMaps = reasoningModel.listSubjectsWithProperty(RDF.type, R2RML.TRIPLESMAP).toList();
    
    for(Resource tripleMap : allTripleMaps){
      
      //get the subject, we assume that the r2rml is valid and therefore has only one subject.
      Resource subject = reasoningModel.listObjectsOfProperty(tripleMap, R2RML.HASSUBJECTMAP).next().asResource();
      
      //get the graph resource
      List<RDFNode> subjectGraphMaps = reasoningModel.listObjectsOfProperty(subject, R2RML.HASGRAPHMAP).toList();
          
      //for all these graph statements
      for(RDFNode graph: subjectGraphMaps){
        for(RDFNode po: reasoningModel.listObjectsOfProperty(tripleMap,R2RML.HASPREDICATEOBJECTMAP).toList()){
          //we add the the graph map into the PO map
          reasoningModel.add(po.asResource(),R2RML.HASGRAPHMAP,graph);
        }
      }
      
      // and remove them from the mapping
      for (RDFNode graph : subjectGraphMaps) {
        reasoningModel.remove(subject,R2RML.HASGRAPHMAP,graph);
      }
    }
    
    
  }


/**
 * read the prepared model into SparqlMap objects.
 * 
 * behold this
 * 
 * @throws R2RMLValidationException
 * @throws JSQLParserException
 */
private Multimap<String,TripleMap> loadTripleMaps(Model reasoningModel) throws R2RMLValidationException,
    JSQLParserException {
  
  int queryCount = 1;
  int tableCount = 1;
  
  Multimap<String,TripleMap> tripleMaps = HashMultimap.create();

  for (Resource tmUri: reasoningModel.listResourcesWithProperty(RDF.type,R2RML.TRIPLESMAP).toList() ) {
    
    List<Statement> tablenames = reasoningModel.listStatements(tmUri, R2RML.HASTABLENAME, (RDFNode) null).toList();
    List<Statement> queries = reasoningModel.listStatements(tmUri,R2RML.HASSQLQUERY,(RDFNode) null).toList();
    List<Statement> version = reasoningModel.listStatements(tmUri,R2RML.SQLVERSION,(RDFNode) null).toList();
    

    FromItem fromItem;
    Table fromTable;
    SubSelect subsel;
    
    if (tablenames.size() == 1 && queries.isEmpty() && version.isEmpty()) {
      String tablename = R2RMLHelper.unescape(tablenames.get(0).getObject().asLiteral().getString());

      fromTable = new Table(null, tablename);
      fromTable.setAlias(tablename);
      log.debug("Table: \"" + tablename + "\" is internally referred to as: \"" +tableCount++ + "\"");
      fromTable.setAlias("table_" + tableCount);
      fromItem = fromTable;
    } else if (tablenames.isEmpty() && queries.size() == 1) {
      String query = R2RMLHelper.cleanSql(queries.get(0).getString());
      subsel = new SubSelect();
      subsel.setAlias("query_" + queryCount++);
      subsel.setSelectBody(new SelectBodyString(query));
      fromTable = new Table(null, subsel.getAlias());
      fromTable.setAlias(subsel.getAlias());
      fromItem = subsel;

    } else {
      throw new R2RMLValidationException(
          "Invalid virtual table declaration in term map: "
              + tmUri.toString());
    }

    // validate fromItem

    

    TripleMap triplemap = new TripleMap(tmUri.getURI(), fromItem);

    // fetch the subject and validate it
    List<Statement> subjectResStmtnt = reasoningModel.listStatements(tmUri, R2RML.HASSUBJECTMAP,(RDFNode) null).toList();
    
    
    // there should only be one
    if (subjectResStmtnt.size() != 1) {
      throw new R2RMLValidationException("Triple map " + tmUri
          + "has " +subjectResStmtnt.size()+"  subject term map, fix this");
    }
    Resource subjectRes = subjectResStmtnt.get(0).getResource();
    
    TermMapQueryResult sres = new TermMapQueryResult(subjectRes, reasoningModel,
        fromItem);
    
    
    //create the subject term map
    
    TermMap stm = null;

    if (sres.termType != null) {
      stm = mapQueryResultOnTermMap(sres, fromItem,triplemap, sres.termType);
    } else {
      stm = mapQueryResultOnTermMap(sres, fromItem,triplemap, R2RML.IRI);
    }
    // some validation
    if (sres.termType != null && sres.termType.equals(R2RML.LITERAL)) {
      throw new R2RMLValidationException(
          "no literal in subject position");
    }
    if (sres.constant != null && !sres.constant.isURIResource()) {
      throw new R2RMLValidationException(
          "Must IRI in predicate position");

    }
    
    stm.trm = triplemap;
    triplemap.setSubject(stm);
    
    
  
    

    List<Statement> postmts = reasoningModel.listStatements(tmUri, R2RML.HASPREDICATEOBJECTMAP, (RDFNode) null).toList();
    
    Map<TermMap,TripleMap> graph2TripleMap = new HashMap<TermMap,TripleMap>(); 
    

    for  (Statement postmt: postmts) {
      Resource poResource = postmt.getResource();
      
      Resource predicateMapResource = reasoningModel.getProperty(poResource, R2RML.HASPREDICATEMAP).getResource();
      Resource objectMapResource = reasoningModel.getProperty(poResource, R2RML.HASOBJECTMAP).getResource();
      
      
      // get the graph statements for the po here.
      List<Statement> graphMapStmts =  reasoningModel.listStatements(poResource, R2RML.HASGRAPHMAP,(RDFNode) null).toList();
      List<TermMap> graphMaps = getGraphmapsForPO(tmUri, fromItem, graphMapStmts);
      
      
      
      
      
      
      
      
  

      TermMapQueryResult p = new TermMapQueryResult(predicateMapResource,reasoningModel,
          fromItem);
      
      
      if (p.termType != null
          && !p.termType.getURI().equals(R2RML.IRI_STRING)) {
        throw new R2RMLValidationException(
            "Only use iris in predicate position");
      }
  
      TermMap ptm = mapQueryResultOnTermMap(p, fromItem,triplemap,R2RML.IRI);
    


      TermMapQueryResult qrO = new TermMapQueryResult(objectMapResource,reasoningModel,
          fromItem);
      //the term type definition according to the R2RML spec  http://www.w3.org/TR/r2rml/#termtype
      
      TermMap otm = null;
    
      //Identify the term type here
      if(qrO.termType != null){
        otm = mapQueryResultOnTermMap(qrO, fromItem,triplemap,qrO.termType);
      }else if(qrO.constant!=null){
        otm = mapQueryResultOnTermMap(qrO, fromItem, triplemap, null);
      }else if(qrO.column != null //when column, etc. then it is a literal
            || qrO.lang != null
            || qrO.datatypeuri != null
            || (qrO.termType != null && qrO.termType.equals(
                R2RML.LITERAL))){
        otm = mapQueryResultOnTermMap(qrO, fromItem, triplemap,R2RML.LITERAL);
        }else{
          //it stays IRI
          
          otm = mapQueryResultOnTermMap(qrO, fromItem, triplemap,R2RML.IRI);
        }
      
      
      for(TermMap graphTermMap: graphMaps){
        TripleMap graphTripleMap = graph2TripleMap.get(graphTermMap);
        if(graphTripleMap==null){
          graphTripleMap = triplemap.getDeepCopy();
          graphTripleMap.setGraph(graphTermMap);
          graph2TripleMap.put(graphTermMap, graphTripleMap);
        }         
        graphTripleMap.addPO(ptm, otm); 
      }       
      tripleMaps.putAll(tmUri.toString(),graph2TripleMap.values());
    }
  }
}
  


public TermMap mapQueryResultOnTermMap(TermMapQueryResult qr, FromItem fi, TripleMap tripleMap, Resource termType) throws R2RMLValidationException{
  
  TermMap tm =  TermMap.createNullTermMap(dth);
  
  if(termType!=null){
    tm.setTermTyp(termType);
  }
  if(qr.constant!=null){
    tm = tfac.createTermMap(qr.constant.asNode());      
  }else if(qr.template!=null){
    if(termType.equals(R2RML.LITERAL)){
    
      List<Expression> resourceExpression = templateToResourceExpression(qr.template, fi, dth);
      tm.literalValString = FilterUtil.concat(resourceExpression.toArray(new Expression[0]));

    }else{
      List<Expression> resourceExpression = templateToResourceExpression(qr.template, fi, dth);
      tm.getResourceColSeg().addAll(resourceExpression);
    }
    
  }else if(qr.column!=null){
    
    Column col = ColumnHelper.createColumn(fi.getAlias(), qr.column);
    
    if(termType.equals(R2RML.LITERAL)){
      
      int sqlType = dbconf.getDataType(fi, qr.column);
          
      if (tm.literalType == null && DataTypeHelper.getRDFDataType(sqlType) != null) {
        tm.setLiteralDataType(DataTypeHelper.getRDFDataType(sqlType).getURI()); 
      }
      RDFDatatype dt = DataTypeHelper.getRDFDataType(sqlType);
      if(dt==null){
        tm.setLiteralDataType(RDFS.Literal.getURI());
      }else{
        tm.setLiteralDataType(dt.getURI());
      }
      
      
      
      if(dth.getCastTypeString(dt).equals(dth.getStringCastType())){
        tm.literalValString = dth.cast(col, dth.getStringCastType());
        
      }else if(dth.getCastTypeString(dt).equals(dth.getNumericCastType())){
        tm.literalValNumeric = dth.cast(col, dth.getNumericCastType());
        
      }else if(dth.getCastTypeString(dt).equals(dth.getBinaryDataType())){
        tm.literalValBinary = dth.cast(col, dth.getBinaryDataType());
        
      }else if(dth.getCastTypeString(dt).equals(dth.getDateCastType())){
        tm.literalValDate = dth.cast(col, dth.getDateCastType());
        
      }else if(dth.getCastTypeString(dt).equals(dth.getBooleanCastType())){
        tm.literalValBool = dth.cast(col, dth.getBooleanCastType());
      }

    }else{
      //tm.resourceColSeg.add(dth.castNull(dth.getStringCastType()));
      tm.resourceColSeg.add(dth.cast(col,dth.getStringCastType()));
    }   
  }
  
  
  if(!tm.isConstant()){
    tm.alias2fromItem.put(fi.getAlias(), fi);
  }
  tm.trm = tripleMap;
  return tm;
}
private void loadParentTripleStatements(Model reasoningModel, Multimap<String, TripleMap> tripleMaps, DataTypeHelper dth) throws R2RMLValidationException {
  
  
  
  
  
  List<Statement> parentTripleMapStatements = reasoningModel.listStatements((Resource)null, R2RML.HASPARENTTRIPLESMAP, (RDFNode) null).toList();
 
  
 for (Statement statement : parentTripleMapStatements) {

   Resource objectMap = statement.getSubject();

   // get the referenced map
   Resource parentTripleMap = statement.getObject().asResource();
   for (TripleMap parentTrM : tripleMaps.get(parentTripleMap.getURI())) {

     // get the child map
     Resource poMap = reasoningModel.listStatements(null, R2RML.HASOBJECTMAP, objectMap).toList().get(0).getSubject();
     Resource mapping = reasoningModel.listStatements(null, R2RML.HASPREDICATEOBJECTMAP, poMap).toList().get(0).getSubject();
     for (TripleMap tripleMap : tripleMaps.get(mapping.getURI())) {

       // we insert this
       TermMap newoTermMap = parentTrM.getSubject().clone("");

       newoTermMap.trm = parentTrM;

       // get the join condition
       List<Statement> joinconditions = reasoningModel.listStatements(objectMap, R2RML.HASJOINCONDITION, (RDFNode) null).toList();
       for (Statement joincondition : joinconditions) {
         Resource joinconditionObject = joincondition.getObject().asResource();

         String parentjc = unescape(reasoningModel.listObjectsOfProperty(joinconditionObject, R2RML.HASPARENT).toList().get(0).asLiteral()
             .getString());
         String childjc = unescape(reasoningModel.listObjectsOfProperty(joinconditionObject, R2RML.HASCHILD).toList().get(0).asLiteral()
             .getString());

         this.dbconf.getDataType(parentTrM.getFrom(), getRealColumnName(parentjc, parentTrM.getFrom()));

         Column leftCol = ColumnHelper.createColumn(parentTrM.getFrom().getAlias(), parentjc);

         Column rightCol = ColumnHelper.createColumn(tripleMap.getFrom().getAlias(), childjc);
         EqualsTo eq = new EqualsTo();
         eq.setLeftExpression(dth.cast(leftCol, dth.getStringCastType()));
         eq.setRightExpression(dth.cast(rightCol, dth.getStringCastType()));
         newoTermMap.getFromJoins().add(eq);
         newoTermMap.addFromItem(tripleMap.getFrom());

         log.debug("Adding join between parent: \"" + parentTrM.getFrom().toString() + "." + parentjc + "\" and child: \"" + tripleMap.getFrom().toString() + "." + childjc + "\"");

       }

       // now we need to create the predicate
       for (RDFNode pnode : reasoningModel.listObjectsOfProperty(poMap, R2RML.HASPREDICATEMAP).toList()) {
         // get the predicate Map
         TermMapQueryResult ptmqr = new TermMapQueryResult(pnode.asResource(), reasoningModel, tripleMap.getFrom());

         TermMap ptm = mapQueryResultOnTermMap(ptmqr, tripleMap.getFrom(), tripleMap, R2RML.IRI);

         tripleMap.addPO(ptm, newoTermMap);

       }
     }
   }

 }

}
  
  /**
   * if a triple map s based on a query, we attempt to decompose it.
   */
  private void decomposeVirtualTableQueries() {
    TERMMAPLOOP: for(TripleMap trm: tripleMaps.values()){
      FromItem fi = trm.getFrom();
      if(fi instanceof SubSelect){
        SelectBody sb  = ((SubSelect) fi).getSelectBody();
        if(sb instanceof SelectBodyString){
          String queryString = ((SelectBodyString) sb).getQuery();
          CCJSqlParser sqlParser = new CCJSqlParser(new StringReader(queryString));
          try {
            sb = sqlParser.SelectBody();
          } catch (ParseException e) {
            log.warn("Could not parse query for optimization " + queryString);
            continue TERMMAPLOOP;
          }   
        }
        
        if(sb instanceof PlainSelect){
          cleanColumnNames((PlainSelect) sb);
          //validate that there are only normal joins on tables are here
          List<Table> tables = new ArrayList<Table>();
          List<EqualsTo> joinConds = new ArrayList<EqualsTo>();
          
          if(!(((PlainSelect) sb).getFromItem() instanceof Table)){
            continue;
          }
          tables.add((Table) ((PlainSelect) sb).getFromItem());
          
          if (((PlainSelect) sb).getJoins() != null) {
            for (Join join : ((PlainSelect) sb).getJoins()) {
              if ((join.isSimple()) || join.isFull()
                  || join.isLeft()
                  || !(join.getRightItem() instanceof Table)) {
                log.warn("Only simple joins can be opzimized");
                continue TERMMAPLOOP;
              }

              Table tab = (Table) join.getRightItem();
              if (tab.getAlias() == null) {
                log.warn("Table: "
                    + tab.getName()
                    + " needs an alias in order to be optimized");
                continue TERMMAPLOOP;
              }

              tables.add(tab);

              // check if we can make use of the on condition.

              Expression onExpr = join.getOnExpression();

              // shaving of parenthesis
              if (onExpr instanceof Parenthesis) {
                onExpr = ((Parenthesis) onExpr).getExpression();
              }

              if (!(onExpr instanceof EqualsTo)) {
                log.warn("only simple equals statements can be processed, aborting optimization ");
                continue TERMMAPLOOP;
              }

              joinConds.add((EqualsTo) onExpr);

            }
          }
          // create a projection map
          Map<String,Column> projections = new HashMap<String,Column>();
          
          for(SelectItem si : ((PlainSelect) sb).getSelectItems()){
            if(si instanceof SelectExpressionItem){
              if(!(((SelectExpressionItem) si).getExpression() instanceof Column)){
                //no  a column in there, so we skip this query
                continue TERMMAPLOOP;
              }
              Column col = (Column) ((SelectExpressionItem) si).getExpression();
              if(col.getTable().getAlias()==null){
                col.getTable().setAlias(col.getTable().getName());
              }
              String alias = ((SelectExpressionItem) si).getAlias();
              projections.put( alias,col ); 
            }
          }
          
          // modify the columns in the term maps
          
          TermMap s = trm.getSubject();
          trm.setSubject(replaceColumn(s, trm, projections, tables, joinConds));
          for(PO po : trm.getPos()){
            po.setObject(replaceColumn(po.getObject(),trm, projections, tables, joinConds));
            po.setPredicate(replaceColumn(po.getPredicate(),trm, projections, tables, joinConds));
          }
          
          log.info("Rewrote query " + trm.getFrom());
          
        }
      }
      
    }
    
  }
  
  private List<TermMap> getGraphmapsForPO(Resource tmUri, FromItem fromItem,
      List<Statement> graphMapStmts) throws R2RMLValidationException {

    List<TermMap> graphMaps = new ArrayList<TermMap>();
    if (graphMapStmts == null || graphMapStmts.isEmpty()) {
      graphMaps = Arrays.asList(this.tfac
          .createTermMap(Quad.defaultGraphNodeGenerated));
    } else {
      for (Statement graphMapStmt : graphMapStmts) {
        List<Expression> graph;

        Resource graphMap = graphMapStmt.getResource();
        if (reasoningModel.contains(graphMap, R2RML.HASTEMPLATE)) {
          String template = reasoningModel.getProperty(graphMap,
              R2RML.HASTEMPLATE).getString();
          graph = templateToResourceExpression(
              cleanTemplate(template, fromItem), fromItem, dth);
        } else if (reasoningModel.contains(graphMap, R2RML.HASCOLUMN)) {
          String column = reasoningModel.getProperty(graphMap,
              R2RML.HASCOLUMN).getString();
          String template = "\"{"
              + getRealColumnName(column, fromItem) + "\"}";
          graph = templateToResourceExpression(
              cleanTemplate(template, fromItem), fromItem, dth);
        } else if (reasoningModel.contains(graphMap, R2RML.HASCONSTANT)) {
          Resource resource = reasoningModel.getProperty(graphMap,
              R2RML.HASCONSTANT).getResource();
          graph = Arrays.asList(resourceToExpression(resource));
        } else {
          throw new R2RMLValidationException(
              "Graphmap without valid value found for "
                  + tmUri.getURI());
        }

        // set the graph
        TermMap gtm = new TermMap(dth);
        gtm.setTermTyp(R2RML.IRI);
        gtm.getResourceColSeg().addAll(graph);

        graphMaps.add(gtm);

      }
    }
    return graphMaps;
  }
  
  
  
//this method analyzes the r2rml file for the most common errors
  private boolean validate(Model reasoningModel) throws R2RMLValidationException{
    
    boolean isValid = true;
    
    //do we have at least one triples map?
    List<Resource> triplesMaps = reasoningModel.listResourcesWithProperty(RDF.type,R2RML.TRIPLESMAP).toList();
    if(triplesMaps.isEmpty()){
      log.error("No triples maps found in this configuration file. Please check, if this is the correct file. Otherwise make sure that at least one triples map is in the file.");
      isValid = false;
    }else{
      //does every triple map have exactly one valid logical table declaration?
      for (Resource tripleMap : triplesMaps) {
        List<RDFNode> logicalTables = reasoningModel.listObjectsOfProperty(tripleMap, R2RML.HASLOGICALTABLE).toList();
        if(logicalTables.isEmpty()){
          throw new R2RMLValidationException("No rr:logicalTable property found for triples map " + tripleMap.getURI());
        }
        for (RDFNode logicalTableNode : logicalTables) {
          if(logicalTableNode.isLiteral()){
            isValid = false;

            throw new R2RMLValidationException("Error in triples map" + tripleMap.getURI() + " rr:logicalTable has a string object. Please use an intermediate node with rr:tableName or rr:sqlQuery.");
          }else{
            Resource logicalTable = logicalTableNode.asResource();
            List<RDFNode> tableNames = reasoningModel.listObjectsOfProperty(logicalTable, R2RML.HASTABLENAME).toList();
            for (RDFNode tableName : tableNames) {
              if(!tableName.isLiteral()){
                isValid = false;

                throw new R2RMLValidationException("tablename of triple map " +tripleMap+ " is not a literal.");
              }
            }
            List<RDFNode> queries = reasoningModel.listObjectsOfProperty(logicalTable, R2RML.HASSQLQUERY).toList();
            for (RDFNode query : queries) {
              if(!query.isLiteral()){
                isValid = false;

                throw new R2RMLValidationException("query of triple map " +tripleMap+ " is not a literal.");
              }
            }
            
            if(tableNames.size() + queries.size()==0){
              throw new R2RMLValidationException("No table name or query is given for triple map " +  tripleMap.getURI());
            }
            if(tableNames.size() + queries.size()>1){
              throw new R2RMLValidationException("Multiple table names or queries are given for triple map " +  tripleMap.getURI());
            }
          }
          
        }
        
        //now checking for the subject map.
        
        List<RDFNode> subjectMaps = reasoningModel.listObjectsOfProperty(tripleMap,R2RML.HASSUBJECTMAP).toList();
        
                
        //now checking for the predicateObject maps.
        
        List<RDFNode> poMaps = reasoningModel.listObjectsOfProperty(tripleMap,R2RML.HASPREDICATEOBJECTMAP).toList();
        if(poMaps.size()==0){
          throw new R2RMLValidationException("No Predicate-Object Maps given for triple map:" + tripleMap.getURI());
        }
        
        for (RDFNode pomap : poMaps) {
          List<RDFNode> predicatemaps =  reasoningModel.listObjectsOfProperty(pomap.asResource(),R2RML.HASPREDICATEMAP).toList();
          if(predicatemaps.size()<1){
            throw new R2RMLValidationException("Found predicateObjectmap without an predicate in triple map: " +  tripleMap.getURI() );
          }
          if(!(predicatemaps.get(0).asResource().hasProperty(R2RML.HASTEMPLATE)
            ||predicatemaps.get(0).asResource().hasProperty(R2RML.HASCONSTANT)
            ||predicatemaps.get(0).asResource().hasProperty(R2RML.HASCOLUMN))){
            throw new R2RMLValidationException("predicate defintion not valid in triples map " + tripleMap.getURI());
          }

          List<RDFNode> objectmaps =  reasoningModel.listObjectsOfProperty(pomap.asResource(),  R2RML.HASOBJECTMAP).toList();
          if(objectmaps.size()<1){
            throw new R2RMLValidationException("Found predicateObjectmap without an object in triple map: " +  tripleMap.getURI() );
          }
          if(!(objectmaps.get(0).asResource().hasProperty(R2RML.HASTEMPLATE)
              ||objectmaps.get(0).asResource().hasProperty(R2RML.HASCONSTANT)
              ||objectmaps.get(0).asResource().hasProperty(R2RML.HASPARENTTRIPLESMAP)
              ||objectmaps.get(0).asResource().hasProperty(R2RML.HASCOLUMN)
              ||(objectmaps.size()>1
               && objectmaps.get(1).asResource().hasProperty(R2RML.HASPARENTTRIPLESMAP))
              )){
              throw new R2RMLValidationException("object defintion not valid in triples map " + tripleMap.getURI());
            }
          
          List<RDFNode> parentTripleMaps = reasoningModel.listObjectsOfProperty(objectmaps.get(0).asResource(), R2RML.HASPARENTTRIPLESMAP).toList();
          if(parentTripleMaps.size()>1){
            if(!parentTripleMaps.get(0).asResource().hasProperty(R2RML.HASLOGICALTABLE)){
              throw new R2RMLValidationException("Triples map " + parentTripleMaps.get(0)+ " is used as parent triples in " + tripleMap.getURI() + " but the referenced resource does not have a rr:logicalTable");
            }
          }
          
        }
      }
    }
    return isValid;
    
  }
  
  
  
  
  private void validatepost() throws R2RMLValidationException{
    // in the end every triple map should have predicate objects.
    for(TripleMap triplemap : tripleMaps.values()){
      if(triplemap.getPos().size()==0){
        throw new R2RMLValidationException("Make sure there are predicate-object maps in triple map: " + triplemap.getUri() );
      }
    }
  }
  
  
}
