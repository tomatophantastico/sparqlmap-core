package org.aksw.sparqlmap.core.spring;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.ServiceLoader;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.aksw.sparqlmap.core.db.DBAccess;
import org.aksw.sparqlmap.core.translate.jdbc.DataTypeHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import com.jolbox.bonecp.BoneCPConfig;
import com.jolbox.bonecp.BoneCPDataSource;

public class DBAccessConfigurator {
  
  static org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(DBAccessConfigurator.class);

  

  static public DBAccess getDBAccess(String dbUrl, String username, String password, Integer poolminconnections,
      Integer poolmaxconnections) throws SQLException{
    
    BoneCPConfig config = createConfig(dbUrl, username, password, poolminconnections, poolmaxconnections);
    BoneCPDataSource bcp = new BoneCPDataSource(config);

    Connection conn = bcp.getConnection();
    String dbname = conn.getMetaData().getDatabaseProductName();
    conn.close();
    
    
    return getDBAccess(dbname,bcp);
    
    
  }

  
  static public DBAccess getDBAccess(Environment env) throws SQLException {

    String dbUrl = env.getProperty("jdbc.url");
    String username = env.getProperty("jdbc.username");
    String password = env.getProperty("jdbc.password");
    Integer poolminconnections = env.getProperty("jdbc.poolminconnections") != null ? Integer.parseInt(env
        .getProperty("jdbc.poolminconnections")) : null;
    Integer poolmaxconnections = env.getProperty("jdbc.poolmaxconnections") != null ? Integer.parseInt(env
        .getProperty("jdbc.poolmaxconnections")) : null;

    BoneCPConfig config = createConfig(dbUrl, username, password, poolminconnections, poolmaxconnections);
    BoneCPDataSource bcp = new BoneCPDataSource(config);

    Connection conn = bcp.getConnection();
    String dbname = conn.getMetaData().getDatabaseProductName();
    conn.close();
    
    
    return getDBAccess(dbname,bcp);

  }

  public static BoneCPConfig createConfig(String dbUrl, String username, String password, Integer poolminconnections,
      Integer poolmaxconnections) {
    if (poolmaxconnections == null) {
      poolmaxconnections = 10;
    }
    if (poolminconnections == null) {
      poolminconnections = 5;
    }

    BoneCPConfig config = new BoneCPConfig();
    config.setJdbcUrl(dbUrl);
    config.setUsername(username);
    config.setPassword(password);
    config.setMinConnectionsPerPartition(poolminconnections);
    config.setMaxConnectionsPerPartition(poolmaxconnections);
    config.setPartitionCount(1);
    return config;
  }


 
  private static DBAccess getDBAccess(String dbname, BoneCPDataSource bcp) {

    Iterator<DBAccess> conns = ServiceLoader.load(DBAccess.class).iterator();
    DBAccess connToUse = null;

    while (conns.hasNext()) {
      DBAccess conn = conns.next();

      if (log.isInfoEnabled()) {
        log.info("Detected :" + conn.getDBName() + " Module, Driver: " + conn.getDriverVersion());
      }

      if (dbname.equals(conn.getDBName())) {
        connToUse = conn;
      }
    }

    if (connToUse == null) {
      throw new Error("Unknown Database " + dbname + " encountered");
    }

    connToUse.setDs(bcp);

    return connToUse;
  }

  
  
  
  



}