package org.aksw.sparqlmap.core.r2rml;

import java.util.Arrays;
import java.util.List;

import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.util.BaseSelectVisitor;
import jersey.repackaged.com.google.common.collect.Lists;
/**
 * contains classes which deal with R2RML constructs and their mapping onto the SparqlMap classes.
 * 
 * @author joerg
 *
 */
public class R2RMLHelper {
  
 public static List<TermMapTemplateStringColumn> splitTemplate(String template) {
    
    List<TermMapTemplateStringColumn> templateSplits = Lists.newArrayList();
    

    List<String> altSeq = Arrays.asList(template
        .split("((?<!\\\\)\\{)|(?<!\\\\)\\}"));

    for (int i = 0; i < altSeq.size(); i++) {
      String value  = altSeq.get(i);
      if (i % 2 == 1) {
        String colname = unescape(value);
        templateSplits.get(templateSplits.size()-1).setColumn(colname);

      } else {
        // static part, no need to change anything, just remove the
        // escape patterns;
         TermMapTemplateStringColumn sc = new TermMapTemplateStringColumn();
         String fixedPart =  value.replaceAll("\\\\", "");
         sc.setString(fixedPart);
         templateSplits.add(sc);


      }
    }

    return templateSplits;
  }

 
 public static String unescape(String toUnescape) {
   if (toUnescape != null && toUnescape.startsWith("\"")
       && toUnescape.endsWith("\"")) {
     return toUnescape.substring(1, toUnescape.length() - 1);
   } else {
     // not escaped, so we need to see how the database handles the
     // string.
     return toUnescape;
   }
 }
 
 
 

 public static String cleanSelectQuery(String toUnescape) {
   if (toUnescape != null) {

     toUnescape = toUnescape.trim();
     toUnescape = toUnescape.replaceAll("\r\n", " ").replaceAll("\n",
         " ");
     if (toUnescape.endsWith(";")) {
       toUnescape = toUnescape.substring(0, toUnescape.length() - 1);
     }

     return toUnescape;
   } else {
     return toUnescape;
   }
 }
 
 public static Resource asResource(String resource){
   return ResourceFactory.createResource(resource);
 }
 
 
}
