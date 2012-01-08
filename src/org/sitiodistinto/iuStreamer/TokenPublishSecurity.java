package org.sitiodistinto.iuStreamer;

import java.util.Map;
import org.red5.server.api.Red5;
import org.red5.server.api.IConnection;
import org.red5.server.api.IScope; 
import org.red5.server.api.stream.IStreamPublishSecurity;
import org.slf4j.Logger;
            
public class TokenPublishSecurity implements IStreamPublishSecurity { 

  private MySqlDatabase database;

  public boolean isPublishAllowed(IScope scope, String name, String mode) {
    boolean status=false;
    String path = scope.getContextPath();

    IConnection conn = Red5.getConnectionLocal();

    try {
      Map<String, Object> connectionParams = conn.getConnectParams();

      if (connectionParams.containsKey("queryString")) {
        //get the raw query string
        String rawQueryString = (String) connectionParams.get("queryString");
        System.out.println("Nos estan pidiendo el token " + rawQueryString);
        //parse into a usable query string
        UrlQueryStringMap<String, String> queryString = UrlQueryStringMap.parse(rawQueryString);

        // Comprueba que el canal y el token coincidan
        String channel = scope.getContextPath().substring(1);
        String token = queryString.get("id");
        status = database.isValidToken(channel,token);
        if (!status) { System.out.println("ERROR: Se ha intentado publicar en el canal " + channel  + " con token incorrecto: " + token); }
      }
    } catch (Exception e) {
      System.out.println("Error autentificando: " + e);
    }

    return status; 
  }

  public void setDatabase(MySqlDatabase db) {
    database = db;
  }

} 
