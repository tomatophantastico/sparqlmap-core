package org.aksw.sparqlmap.core.r2rml;

import java.util.List;

import org.aksw.sparqlmap.core.r2rml.TermMapReferencing.JoinOn;
import org.apache.metamodel.data.FirstRowDataSet;

import com.google.common.collect.Lists;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;

public class TermMapLoader {
    


    public static TermMap load(Model r2rmlmodel, Resource termMap) {
      
      TermMap result = null;
      
      String defaultPrefix = r2rmlmodel.getNsPrefixURI("");
      
      Resource termType = LoaderHelper.getSingleResourceObject(
          r2rmlmodel.listStatements(termMap, R2RML.TERMTYPE, (RDFNode) null));
      
      String column = LoaderHelper.getSingleLiteralObjectValue(
          r2rmlmodel.listStatements(termMap, R2RML.HASCOLUMN, (RDFNode) null));
      String template = LoaderHelper.getSingleLiteralObjectValue(
          r2rmlmodel.listStatements(termMap, R2RML.HASTEMPLATE, (RDFNode) null));
      
      RDFNode constant = LoaderHelper.getSingleRDFNode(
          r2rmlmodel.listStatements(termMap, R2RML.HASCONSTANT, (RDFNode) null));
      
      String language = LoaderHelper.getSingleLiteralObjectValue(
          r2rmlmodel.listStatements(termMap, R2RML.HASLANGUAGE, (RDFNode) null));
      
      Resource datatype = LoaderHelper.getSingleResourceObject(
          r2rmlmodel.listStatements(termMap, R2RML.HASDATATYPE, (RDFNode) null));
      
      Resource parentMap = LoaderHelper.getSingleResourceObject(
          r2rmlmodel.listStatements(termMap, R2RML.HASPARENTTRIPLESMAP, (RDFNode) null));
      
      
      //if not explicitly declared, we infer from the location (g,s,p,o) the term type
      if(termType==null){
        if(r2rmlmodel.contains(null, R2RML.HASOBJECTMAP, termMap)){
          if(column!=null || language != null || datatype !=null){
            termType = R2RML.LITERAL;
          }else{
            
          }
        }else{
          termType = R2RML.IRI;
        }
      }
      
      
      if(column!=null&&template==null&&constant==null&&parentMap==null){
        TermMapColumn tmCol = new TermMapColumn();
        tmCol.setColumn(R2RMLHelper.unescape(column));
        result = tmCol;
      }else if(column==null&&template!=null&&constant==null&&parentMap==null){
        TermMapTemplate tmTemplate = new TermMapTemplate();
        tmTemplate.setTemplate(R2RMLHelper.splitTemplate(template));
        // expand the template with the base prefix
        String firstTemplateString = tmTemplate.getTemplate().get(0).getString();
        if(!firstTemplateString.contains(":")){
          String newTemplateString = defaultPrefix + firstTemplateString;
          tmTemplate.getTemplate().get(0).setString(newTemplateString);
        }
        
        result = tmTemplate;
      }else if(column==null&&template==null&&constant!=null&&parentMap==null){
        TermMapConstant tmConst = new TermMapConstant();
        if(constant.isURIResource()){
          tmConst.setConstantIRI(constant.asResource().getURI());
        }else if(constant.isLiteral()){
          tmConst.setConstantLiteral(constant.asLiteral().getLexicalForm());
          tmConst.setLang(constant.asLiteral().getLanguage());
          tmConst.setDatatypIRI(constant.asLiteral().getDatatypeURI());
        }else{
          throw new R2RMLValidationException("Blank node is not valid constant value for term map");
        }
        result = tmConst;
      }else if(column==null&&template==null&&constant==null&&parentMap!=null){
          TermMapReferencing tmRef = new TermMapReferencing();
          
          
          //and the table or query
          
          Resource logicaTable = LoaderHelper.getSingleResourceObject(
              r2rmlmodel.listStatements(parentMap, R2RML.HASLOGICALTABLE, (RDFNode) null));
          String table = LoaderHelper.getSingleLiteralObjectValue(
              r2rmlmodel.listStatements(logicaTable, R2RML.HASTABLENAME,(RDFNode) null));
          String query = LoaderHelper.getSingleLiteralObjectValue(
              r2rmlmodel.listStatements(logicaTable, R2RML.HASSQLQUERY,(RDFNode) null));

          //get all the join conditions
          List<TermMapReferencing.JoinOn> joinons = Lists.newArrayList();
          List<Statement> conditions = parentMap.listProperties(R2RML.HASJOINCONDITION).toList();
          for(Statement condition:  conditions){
            if(condition.getObject().isResource()){
              Resource condResource = condition.getObject().asResource();
              
              String parentCol = LoaderHelper.getSingleLiteralObjectValue( condResource.listProperties(R2RML.HASPARENT));
              String childCol = LoaderHelper.getSingleLiteralObjectValue( condResource.listProperties(R2RML.HASCHILD));
              
              
              JoinOn joinon = new JoinOn();
              joinon.setChildColumn(childCol);
              joinon.setParentColumn(parentCol);
              joinons.add(joinon);
            }
          }
          tmRef.setConditions(joinons);
          
          // get the query or table name
          
          result = tmRef;
        
      }else{  
        throw new R2RMLValidationException("Check termmap definition for multiple or lacking definitons of rr:constant, rr:template or rr:column");
      }
      
      
      
      return result;
    }
    
    
    public static TermMap defaultGraphTermMap(){
      TermMapConstant dgTermMap = new TermMapConstant();
      dgTermMap.setConstantIRI(R2RML.DEFAULTGRAPH_STRING);
      return dgTermMap;
    }
}
