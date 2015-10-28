package org.aksw.sparqlmap.core.r2rml;

import java.sql.Timestamp;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimestampValue;

import org.aksw.sparqlmap.core.ImplementationException;
import org.aksw.sparqlmap.core.mapper.translate.DataTypeHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.hp.hpl.jena.datatypes.RDFDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDatatype;
import com.hp.hpl.jena.datatypes.xsd.XSDDateTime;
import com.hp.hpl.jena.graph.Node;
import com.hp.hpl.jena.graph.impl.LiteralLabel;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.vocabulary.RDFS;

/**
 * This factory creates JDBC TermMaps, i.e. TermMaps that are bound to a specific database. 
 * 
 * @author joerg
 *
 */
@Component
public class JDBCTermMapFactory {
	
	@Autowired
	DataTypeHelper dth;
	public void setDth(DataTypeHelper dth) {
		this.dth = dth;
	}

	
	/**
	 * create a TermMap for a static node.
	 * @param node
	 * @return
	 */
	public JDBCTermMap createTermMap(Node node){
		
		JDBCTermMap tm = new JDBCTermMap(dth);
	
		if(node.isLiteral()){
			tm.setTermTyp(R2RML.Literal);
			RDFDatatype dt = node.getLiteralDatatype();
	     

	
			LiteralLabel constLit = node.getLiteral();
			if(dt==null){
				tm.setLiteralDataType(RDFS.Literal.getURI());
			}else{
				tm.setLiteralDataType(dt.getURI());
			}
			
			Resource dtResource = null;
			if(dt!=null){
        dtResource = ResourceFactory.createResource(dt.getURI());
      }
			
			// set the value here
			if(dth.getCastTypeString(dtResource).equals(dth.getStringCastType())){
				StringValue stringVal = new StringValue("'"+constLit.getLexicalForm()+"'");
				tm.literalValString = dth.cast( stringVal, dth.getStringCastType());
				
			}else if(dth.getCastTypeString(dtResource).equals(dth.getNumericCastType())){
				LongValue longValue  = new LongValue(constLit.getLexicalForm());
				tm.literalValNumeric = dth.cast(longValue, dth.getNumericCastType());
				
			}else if(dth.getCastTypeString(dtResource).equals(dth.getBinaryDataType())){
				StringValue binVal = new StringValue("'"+constLit.getLexicalForm()+"'");
				tm.literalValBinary = dth.cast(binVal, dth.getBinaryDataType());
				
			}else if(dth.getCastTypeString(dtResource).equals(dth.getDateCastType())){
				Long timestamp;
				Object value = constLit.getValue();
				if(value  instanceof XSDDateTime){
					
					timestamp  = ((XSDDateTime) value).asCalendar().getTimeInMillis();
				}else{
					throw new ImplementationException("Encountered unkown datatype as data:" + value.getClass());
				}
				
				
				TimestampValue dateValue = new TimestampValue(new Timestamp(timestamp)); 
				tm.literalValDate = dth.cast(dateValue, dth.getDateCastType());
				
			}else if(dth.getCastTypeString(dtResource).equals(dth.getBooleanCastType())){
				StringValue bool = new StringValue("'"+constLit.getLexicalForm()+"'");
				tm.literalValBool = dth.cast(bool, dth.getBooleanCastType());
			}
			 
			
		}else{
			//not a Literal, so it has to be a resource
			tm.getResourceColSeg().add(resourceToExpression(node.getURI()));
			
			if(node.isBlank()){
				tm.setTermTyp(R2RML.BlankNode);
			}else{
				tm.setTermTyp(R2RML.IRI);
			}
		}
		
		return tm;
	}
	
	
	
	public JDBCTermMap createBoolTermMap(Expression bool){
		JDBCTermMap tm = new JDBCTermMap(dth);
		tm.setTermTyp(R2RML.Literal);
		tm.setLiteralDataType(XSDDatatype.XSDboolean.getURI());
		tm.literalValBool = dth.cast(bool, dth.getBooleanCastType());
		
		return tm;
	}
	
	public JDBCTermMap createStringTermMap(Expression string){
		JDBCTermMap tm = new JDBCTermMap(dth);
		tm.setTermTyp(R2RML.Literal);
		tm.setLiteralDataType(RDFS.Literal.getURI());
		tm.literalValString = dth.cast(string, dth.getStringCastType());
		
		return tm;
	}
	
	public JDBCTermMap createNumericalTermMap(Expression numeric,Expression datatype){
		JDBCTermMap tm = new JDBCTermMap(dth);
		tm.setTermTyp(R2RML.Literal);
		tm.literalType = datatype;
		tm.literalValNumeric = dth.cast(numeric, dth.getNumericCastType());
		
		return tm;
	}
	
	 public JDBCTermMap createNumericalTermMap(Expression numeric,XSDDatatype datatype){
	    JDBCTermMap tm = new JDBCTermMap(dth);
	    tm.setTermTyp(R2RML.Literal);
	    tm.setLiteralDataType(datatype.getURI());
	    tm.literalValNumeric = dth.cast(numeric, dth.getNumericCastType());
	    
	    return tm;
	  }
	
	
	
	private Expression resourceToExpression(String uri){
		return dth.cast(new StringValue("\"" + uri + "\""), dth.getStringCastType());	
	}
	


}
