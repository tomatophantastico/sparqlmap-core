package org.aksw.sparqlmap;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.metamodel.UpdateableDataContext;
import org.apache.metamodel.jdbc.JdbcDataContext;
import org.apache.metamodel.schema.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;


/**
 * A Collection of methods conveniently bundeled for different db vendors.
 * Useful for handling test databases.
 * Do not use in production databases.
 * @author joerg
 *
 */
public class DBHelper {
  
  private static Logger log = LoggerFactory.getLogger(DBHelper.class);
  
  
  public static void flushDb(Connection conn){
    UpdateableDataContext dc = new JdbcDataContext(conn);
    
    for (Table table: dc.getDefaultSchema().getTables()){
      try {
        java.sql.Statement stmt = conn.createStatement();
        stmt.execute("DROP TABLE \"" + table.getName() + "\" CASCADE");
        stmt.close();

      } catch (SQLException e) {
        log.info("brute force delete threw error, nothing unusual");
      }
    }
  }
  
  public static void flushDbOracle(Connection conn){
    UpdateableDataContext dc = new JdbcDataContext(conn);
    
    for (Table table: dc.getDefaultSchema().getTables()){
      try {
        java.sql.Statement stmt = conn.createStatement();
        stmt.execute("DROP TABLE \"" + table.getName() + "\" CASCADE CONSTRAINTS");
        stmt.close();

      } catch (SQLException e) {
        log.info("brute force delete threw error, nothing unusual");
      }
    }
  }
  
  public static void loadSqlFile(Connection conn, String file) throws SQLException{
    
    ResourceDatabasePopulator rdp = new ResourceDatabasePopulator();
    rdp.addScript(new FileSystemResource(file));
    conn.setAutoCommit(true);
    rdp.populate(conn);
    conn.close();
  }

}
