package org.aksw.sparqlmap;

import java.net.URI;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Assume;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jersey.repackaged.com.google.common.collect.Sets;

import com.google.common.collect.Lists;
import com.spotify.docker.client.DefaultDockerClient;
import com.spotify.docker.client.DockerCertificateException;
import com.spotify.docker.client.DockerCertificates;
import com.spotify.docker.client.DockerClient;
import com.spotify.docker.client.DockerException;
import com.spotify.docker.client.DockerClient.ListContainersParam;
import com.spotify.docker.client.messages.Container;
import com.spotify.docker.client.messages.ContainerConfig;
import com.spotify.docker.client.messages.ContainerCreation;
import com.spotify.docker.client.messages.ContainerInfo;
import com.spotify.docker.client.messages.HostConfig;
import com.spotify.docker.client.messages.PortBinding;

/**
 * This class contains all methods and configuration options to start Docker containers in a static way.
 * 
 * 
 * @author joerg
 *
 */
public class DockerHelper {
  
  
  private static final String MYSQL_DOCKER_CONTAINER_NAME = "sparqlmap_mysql_test";
  private static final String MYSQL_DOCKER_IMAGE_NAME = "mysql";
  
  private static final String POSTGRES_DOCKER_CONTAINER_NAME = "sparqlmap_postgres_test";
  private static final String POSTGRES_DOCKER_IMAGE_NAME = "postgres";
  
  
  
  private static Logger log = LoggerFactory.getLogger(DockerHelper.class);
  /**
   * Instantiate a docker connection, either by reading the env variables or against the default boot2docker ip.
   * 
   * @return
   * @throws InterruptedException 
   * @throws DockerException 
   */
  public static DockerClient startDocker(String containername,String imagename,List<String> ports,List<String> env) throws DockerException, InterruptedException{
    DockerClient docker = acquireDocker();
    
 //try to clean old docker containter
    
    stopDocker( containername);
    
    
    Map<String, List<PortBinding>> portBindings = new HashMap<String, List<PortBinding>>();
    for (String port : ports) {
        List<PortBinding> hostPorts = Lists.newArrayList();
        hostPorts.add(PortBinding.of("0.0.0.0", port));
        portBindings.put(port, hostPorts);
    }
   
    HostConfig hostConfig = HostConfig.builder().portBindings(portBindings).build();
    
    
    ContainerConfig containerConfig = ContainerConfig.builder()
        .image(imagename).exposedPorts(Sets.newHashSet(ports))
        .env(env)
      
        .build();

    ContainerCreation creation = docker.createContainer(containerConfig,containername);
    String dockerId = creation.id();
    


    docker.startContainer(dockerId, hostConfig);
    Thread.sleep(200);
    ContainerInfo info = docker.inspectContainer(dockerId);
    if(!info.state().running()){
      throw new DockerException("Failed to start container");
    }
    
    return docker;
  }

  public static DockerClient acquireDocker() {
    DockerClient docker = null;
    try{
      DockerClient dockerEnv = DefaultDockerClient.fromEnv().build();
      dockerEnv.ping();
      docker = dockerEnv;
    }catch(DockerException e){
      log.warn("Failed to acquire connection via environment variables");
    } catch (InterruptedException e) {
     log.error("Interruption",e);
    } catch (DockerCertificateException e) {
     log.error("Failing ",e);
    }
    
    if(docker==null){
      
      try{
        DockerClient dockerEnv = DefaultDockerClient.builder()
            .uri(URI.create("https://192.168.99.100:2376"))
            .dockerCertificates(
                new DockerCertificates(Paths.get(System.getProperty("user.home") +  "/.docker/machine/machines/default")))
            .build();
        dockerEnv.ping();
        docker = dockerEnv;
      }catch(DockerException e){
        log.warn("Failed to acquire connection via environment variables",e);
      } catch (InterruptedException e) {
       log.error("Interruption",e);
      } catch (DockerCertificateException e) {
       log.error("Failed to get Docker Certificates ",e);
      }
    }
    
    return docker;
  }
  
  public static void stopDocker(String name) throws DockerException, InterruptedException{
    DockerClient docker = acquireDocker();
    if(docker!=null){
    
        for (Container container: docker.listContainers(ListContainersParam.allContainers())){
          if(container.names().contains("/"+name)){
            docker.killContainer(container.id());
            docker.removeContainer(container.id(),true);
          }
        }
    }
  

    
  }
  
  
  
  
  /**
   * get the host component of the host uri.
   * 
   * either get what is defined over variables, or the default boot2docker ip.
   * @return
   */
  public static String getDockerHost() {
    String host = "192.168.99.100";
    try {
      if(DefaultDockerClient.fromEnv().uri() != null){
        
        host = DefaultDockerClient.fromEnv().build().ping();
        DefaultDockerClient.fromEnv().uri().getHost();
      }
    } catch (Exception e) {

    }  

   return host;
  }
  
  public static DBConnConfig startMySQLDocker() throws DockerException, InterruptedException {
    // this approach will work for most dev setups
     
     String url = getDockerHost();
     DBConnConfig dbProps = new DBConnConfig();
     dbProps.jdbcString = String.format("jdbc:mysql://%s:3306/sparqlmaptest?padCharsWithSpace=true&sessionVariables=sql_mode='ANSI_QUOTES'", url );
     dbProps.username = "sparqlmap";
     dbProps.password = "sparqlmap";
     
     List<String> ports = Lists.newArrayList("3306");
     List<String> env = Lists.newArrayList("MYSQL_ROOT_PASSWORD=root","MYSQL_PASSWORD=sparqlmap","MYSQL_USER=sparqlmap","MYSQL_DATABASE=sparqlmaptest");
     DockerClient docker = startDocker(MYSQL_DOCKER_CONTAINER_NAME,MYSQL_DOCKER_IMAGE_NAME,ports,env);
     
     return dbProps;
   }
  public static class DBConnConfig{

    public String password;
    public String username;
    public String jdbcString;
    
  }
  public static void stopMySQLDocker() throws DockerException, InterruptedException {
    stopDocker(MYSQL_DOCKER_CONTAINER_NAME);
  }

  public static DBConnConfig startPostGresDocker() throws DockerException, InterruptedException {
   // this approach will work for most dev setups
    DBConnConfig result = new DBConnConfig();
    String url = getDockerHost();
    
    
    
    result.jdbcString = String.format("jdbc:postgresql://%s:5432/postgres", url );

    result.username = "postgres";
    result.password = "postgres";
    
    List<String> ports = Lists.newArrayList("5432");
    List<String> env = Lists.newArrayList("POSTGRES_PASSWORD=postgres");
    DockerClient docker = startDocker(POSTGRES_DOCKER_CONTAINER_NAME,POSTGRES_DOCKER_IMAGE_NAME,ports,env);
    
    return result;
    
  }

  public static void stopPostGresDocker() throws DockerException, InterruptedException {
    stopDocker(POSTGRES_DOCKER_CONTAINER_NAME);
  }
}
