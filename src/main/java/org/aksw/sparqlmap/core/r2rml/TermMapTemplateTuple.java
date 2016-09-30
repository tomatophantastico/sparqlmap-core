package org.aksw.sparqlmap.core.r2rml;

import org.apache.metamodel.schema.Column;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;


@Data
@Builder
public class TermMapTemplateTuple {
  

    private String column;
    private String prefix;
    private boolean colUrlEncoding = true;

   
    

}
