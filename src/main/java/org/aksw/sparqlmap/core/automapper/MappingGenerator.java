package org.aksw.sparqlmap.core.automapper;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.aksw.sparqlmap.core.r2rml.R2RML;
import org.apache.metamodel.schema.Column;
import org.apache.metamodel.schema.Relationship;
import org.apache.metamodel.schema.Schema;
import org.apache.metamodel.schema.Table;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.net.UrlEscapers;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;

/**
 * Creates a Direct Mapping for the given schema
 * 
 * @author joerg
 *
 */
public class MappingGenerator {
  
  
  private String mappingPrefix;
  private String instancePrefix;
  private String vocabularyPrefix;
  private String primaryKeySeparator;
  
  
  public MappingGenerator(String mappingPrefix, String instancePrefix,
      String vocabularyPrefix, String primaryKeySeparator) {
    super();
    this.mappingPrefix = mappingPrefix;
    this.instancePrefix = instancePrefix;
    this.vocabularyPrefix = vocabularyPrefix;
    this.primaryKeySeparator = primaryKeySeparator;
  }
  
  
  public Model generateMapping(Schema schema) throws UnsupportedEncodingException{
   Model r2r = initMappingModel();

   for(Table table: schema.getTables()){
     Resource triplesMap = r2r.createResource(mappingPrefix + "mapping/" + ues(table.getName()));
     
     //add the logical Table statment
     Resource rrTableName = r2r.createResource();
     r2r.add(triplesMap,R2RML.HASLOGICALTABLE,rrTableName);
     r2r.add(rrTableName,R2RML.HASTABLENAME, escapeName(table.getName()));
     
     //add the subject map
     
     String subjectTemplate = generateSubjectTemplate(table);
     Resource subjectMap = r2r.createResource();
     r2r.add(triplesMap, R2RML.HASSUBJECTMAP, subjectMap);
     r2r.add(subjectMap,R2RML.HASTEMPLATE,subjectTemplate);
     
     //if no primary present, generate a blank node
     if(table.getPrimaryKeys().length==0){
       subjectMap.addProperty(R2RML.TERMTYPE,R2RML.BLANKNODE);

     }
     
     
     // and the class statement
     r2r.add(subjectMap,R2RML.HASCLASS,r2r.createResource(vocabularyPrefix + ues(table.getName())));
     
     
     // map all relations 
     for(Relationship relationship: table.getForeignKeyRelationships() ){

       Resource pomap  = r2r.createResource();
       triplesMap.addProperty(R2RML.HASPREDICATEOBJECTMAP,pomap);
       
       // generate the property
       List<String> cols = Lists.transform(Lists.newArrayList(relationship.getForeignColumns()), new Function<Column,String>(){
        @Override
        public String apply(Column input) {
          return ues(input.getName());
        }
       });
       String refMapPropertySuffix = Joiner.on(this.primaryKeySeparator).join(cols);
       pomap.addProperty(R2RML.HASPREDICATE, r2r.createResource(vocabularyPrefix +ues(relationship.getForeignTable().getName()) +"#ref-"+ refMapPropertySuffix));
       
       // generate the object triple map condition
       Resource objectMap = r2r.createResource();
       pomap.addProperty(R2RML.HASOBJECTMAP, objectMap);
       
       objectMap.addProperty(R2RML.HASPARENTTRIPLESMAP, r2r.createResource(mappingPrefix + "mapping/" + ues(relationship.getPrimaryTable().getName())));
       
       for(int i = 0; i<relationship.getForeignColumns().length;i++  ){
         Resource joinCondition = r2r.createResource();
         objectMap.addProperty(R2RML.HASJOINCONDITION,joinCondition);
         joinCondition.addLiteral(R2RML.HASPARENT, this.escapeName(relationship.getPrimaryColumns()[i].getName()));
         joinCondition.addLiteral(R2RML.HASCHILD, this.escapeName(relationship.getForeignColumns()[i].getName()));
       }
     }
     
     // map all data columns 
     for(Column column : table.getColumns()){
       Resource pomap  = r2r.createResource();
       triplesMap.addProperty(R2RML.HASPREDICATEOBJECTMAP,pomap);
       pomap.addProperty(R2RML.HASPREDICATE, r2r.createResource(vocabularyPrefix +ues(column.getTable().getName()) +"#"+ ues(column.getName())));       
       Resource objectMap = r2r.createResource();
       pomap.addProperty(R2RML.HASOBJECTMAP, objectMap);
       objectMap.addProperty(R2RML.HASCOLUMN, escapeName(column.getName()));
     }
    
     
     
     
   }
    
    
    
    
    
    
    return r2r;
  }
  
  
  
  
  Model initMappingModel(){
    Model r2rmlMapping = ModelFactory.createDefaultModel();
    
    r2rmlMapping.setNsPrefix("rr", R2RML.R2RML_STRING);
    r2rmlMapping.setNsPrefix("vocab", vocabularyPrefix);
    r2rmlMapping.setNsPrefix("mapping", mappingPrefix);
    r2rmlMapping.setNsPrefix("inst", instancePrefix);
    return r2rmlMapping;
  }

 
  
  private String generateSubjectTemplate(Table table){
    List<Column> colsOfSubject = null;

    if(table.getPrimaryKeys().length>0){
      //join the primary keys to generate the subject pattern
      colsOfSubject= Lists.newArrayList(( table.getPrimaryKeys()));

    }else{
      //use all cols to generate subject
      colsOfSubject= Lists.newArrayList(( table.getColumns()));
    }
    
    Function<Column,String> funcCol2Colname = new Function<Column,String>() {
      @Override
      public String apply(Column input) {
        return String.format("%s=%s",ues(input.getName()), escapeAsTemplate(input.getName()));
      }
     };
    
    String templateSuffix =
        Joiner.on(primaryKeySeparator).join(
            Lists.transform(
                colsOfSubject, funcCol2Colname
                ) );
    
    String template = this.instancePrefix + ues(table.getName()) +"/" + templateSuffix;
    
    return template;
  }
  
  
  private String ues(String segment) {
    return UrlEscapers.urlPathSegmentEscaper().escape(segment);
  }
  
  private String escapeAsTemplate(String str){
    return String.format("{\"%s\"}", str);

  }
  
  private String escapeName(String str){
    return String.format("\"%s\"", str);
  }
  
 

}
