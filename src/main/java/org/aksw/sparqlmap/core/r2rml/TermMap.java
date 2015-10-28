package org.aksw.sparqlmap.core.r2rml;

import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.Resource;

public class TermMap {
    private String[] template;
    private String column;
    private RDFNode constant;
    private String lang;
    private Resource datatypeuri;
    private String inverseExpression;
    private Resource termType;
     
   
    public TermMap() {
    }

    public String[] getTemplate() {
        return template;
    }

    public void setTemplate(String[] template) {
        this.template = template;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public RDFNode getConstant() {
        return constant;
    }

    public void setConstant(RDFNode constant) {
        this.constant = constant;
    }

    public String getLang() {
        return lang;
    }

    public void setLang(String lang) {
        this.lang = lang;
    }

    public Resource getDatatypeuri() {
        return datatypeuri;
    }

    public void setDatatypeuri(Resource datatypeuri) {
        this.datatypeuri = datatypeuri;
    }

    public String getInverseExpression() {
        return inverseExpression;
    }

    public void setInverseExpression(String inverseExpression) {
        this.inverseExpression = inverseExpression;
    }

    public Resource getTermType() {
        return termType;
    }

    public void setTermType(Resource termType) {
        this.termType = termType;
    }
}