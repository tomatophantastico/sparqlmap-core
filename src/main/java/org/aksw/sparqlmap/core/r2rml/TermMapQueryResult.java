package org.aksw.sparqlmap.core.r2rml;
import net.sf.jsqlparser.statement.select.FromItem;

import org.aksw.sparqlmap.core.exception.R2RMLValidationException;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;

public class TermMapQueryResult {
    
    
    
    public TermMapQueryResult(Resource tm, Model model, FromItem fi) throws R2RMLValidationException{
      template = model.listObjectsOfProperty(tm, R2RML.HASTEMPLATE).hasNext()?cleanTemplate(model.listObjectsOfProperty(tm, R2RML.HASTEMPLATE).next().asLiteral().getString(),fi):null;
      
      column = model.listObjectsOfProperty(tm, R2RML.HASCOLUMN).hasNext()?getRealColumnName(model.listObjectsOfProperty(tm, R2RML.HASCOLUMN).next().asLiteral().getString(), fi):null;
      
      lang = model.listObjectsOfProperty(tm, R2RML.HASLANGUAGE).hasNext()?model.listObjectsOfProperty(tm, R2RML.HASLANGUAGE).next().asLiteral().getString():null;
      
      inverseExpression = model.listObjectsOfProperty(tm, R2RML.HASINVERSEEXPRESSION).hasNext()?model.listObjectsOfProperty(tm, R2RML.HASINVERSEEXPRESSION).next().asLiteral().getString():null;
      constant = model.listObjectsOfProperty(tm, R2RML.HASCONSTANT).hasNext()?model.listObjectsOfProperty(tm, R2RML.HASCONSTANT).next():null;
      datatypeuri =model.listObjectsOfProperty(tm, R2RML.HASDATATYPE).hasNext()?model.listObjectsOfProperty(tm, R2RML.HASDATATYPE).next().asResource():null;    
      termType =  model.listObjectsOfProperty(tm, R2RML.TERMTYPE).hasNext()?model.listObjectsOfProperty(tm, R2RML.TERMTYPE).next().asResource():null;
      
      
    }

    
    String[] template;
    String column;
    RDFNode constant;
    String lang;
    Resource datatypeuri;
    String inverseExpression;
    Resource termType;
    
    
  }