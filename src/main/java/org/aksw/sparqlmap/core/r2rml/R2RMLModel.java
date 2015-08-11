package org.aksw.sparqlmap.core.r2rml;

import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
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
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.util.BaseSelectVisitor;

import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.exception.ImplementationException;
import org.aksw.sparqlmap.core.exception.R2RMLValidationException;
import org.aksw.sparqlmap.core.mapper.compatibility.CompatibilityChecker;
import org.aksw.sparqlmap.core.mapper.translate.ColumnHelper;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.aksw.sparqlmap.core.mapper.translate.FilterUtil;
import org.aksw.sparqlmap.core.r2rml.TripleMap.PO;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
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
/**
 * This is a wrapper around a multimap that with all the triples maps
 * 
 * @author joerg
 *
 */
public class R2RMLModel {
	
	static org.slf4j.Logger log = org.slf4j.LoggerFactory
			.getLogger(R2RMLModel.class);
	

	Multimap<String,TripleMap> tripleMaps = HashMultimap.create();
	
	


	public R2RMLModel(Multimap<String, TripleMap> tripleMaps) {
    super();
    this.tripleMaps = tripleMaps;
  }




  public Set<TripleMap> getTripleMaps() {
		return new HashSet<TripleMap>(tripleMaps.values());
	}
	

	
}
