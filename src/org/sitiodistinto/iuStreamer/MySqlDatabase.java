package org.sitiodistinto.iuStreamer;

import java.util.Map;
import java.sql.*;
import com.mysql.jdbc.Driver;
import org.red5.server.api.statistics.IClientBroadcastStreamStatistics;
            
public class MySqlDatabase { 

  private Connection conn;

  public MySqlDatabase(String dbname, String dbuser, String dbpass) {
    try {
      // Conectamos con la BBDD
      conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/"+dbname+"?autoReconnect=true",dbuser,dbpass);
      // Limpiamos cualquier conexion de video live que hubiera pendiente
      PreparedStatement stm = conn.prepareStatement("UPDATE videos SET live = FALSE ;");
      stm.execute();
    } catch (SQLException ex) {
      System.out.println("-------------> ERROR!!!");
      System.out.println("SQLException: " + ex.getMessage());
      System.out.println("SQLState: " + ex.getSQLState());
      System.out.println("VendorError: " + ex.getErrorCode());
    } 
  } 

  // Registramos el conector a la BBDD
  static {
    try {
      Class.forName("com.mysql.jdbc.Driver");
    } catch ( ClassNotFoundException exception ) {
      System.out.println( "---------------> ERROR: ClassNotFoundException " + exception.getMessage( ) );
    }
  }

  // Registramos un video en BBDD
  public void startRecord(String channel, String filename) {
    try {
      // Obtenemos el ID del canal 
      PreparedStatement stm = conn.prepareStatement("SELECT id FROM channels WHERE name= ? ;");
      stm.setString(1, channel);
      ResultSet rs = stm.executeQuery();
      if (rs.next()) {
        int channel_id = rs.getInt(1);
        stm = conn.prepareStatement("INSERT INTO videos SET channel_id= ?, live = TRUE, views = 1, filename= ? , created_at= CURRENT_TIMESTAMP ;");
        stm.setInt(1, channel_id); 
        stm.setString(2, filename);
        stm.execute();
      }
    } catch (SQLException ex) {
      System.out.println("-------------> ERROR!!!");
      System.out.println("SQLException: " + ex.getMessage());
      System.out.println("SQLState: " + ex.getSQLState());
      System.out.println("VendorError: " + ex.getErrorCode());
    }
  }

  // Le quita el flag live al video en BBDD y pone la duracion
  public void stopRecord(String filename, long duration) {
    try {
      // Obtenemos el ID del canal 
      PreparedStatement stm = conn.prepareStatement("UPDATE videos SET live = FALSE, duration= ?, viewers=views WHERE filename= ? ;");
      stm.setLong(1, duration/1000);
      stm.setString(2, filename);
      stm.execute();
    } catch (SQLException ex) {
      System.out.println("-------------> ERROR!!!");
      System.out.println("SQLException: " + ex.getMessage());
      System.out.println("SQLState: " + ex.getSQLState());
      System.out.println("VendorError: " + ex.getErrorCode());
    }
  }

  // Comprueba que el token de emision de un canal es correcto
  public boolean isValidToken(String channel, String token) {
    try {
      // Hacemos la consulta
      PreparedStatement stm = conn.prepareStatement("SELECT stream_token FROM channels WHERE name= ? AND stream_token= ? ;");
      stm.setString(1, channel);
      stm.setString(2, token);
      ResultSet rs = stm.executeQuery();
      if (rs.next()) {
        if (rs.getString(1).equals(token)) { return true; }
      }
    } catch (SQLException ex) {
      System.out.println("-------------> ERROR!!!");
      System.out.println("SQLException: " + ex.getMessage());
      System.out.println("SQLState: " + ex.getSQLState());
      System.out.println("VendorError: " + ex.getErrorCode());
    }
    return false;
  }

  // Comprueba que el video enlatado pueda verse
  public boolean isVODVideoPublic(String filename) {
    try {
      // Hacemos la consulta
      PreparedStatement stm = conn.prepareStatement("SELECT public FROM videos WHERE filename= ? ;");
      stm.setString(1, filename);
      ResultSet rs = stm.executeQuery();
      if (rs.next()) {
        return rs.getBoolean(1);
      }
    } catch (SQLException ex) {
      System.out.println("-------------> ERROR!!!");
      System.out.println("SQLException: " + ex.getMessage());
      System.out.println("SQLState: " + ex.getSQLState());
      System.out.println("VendorError: " + ex.getErrorCode());
    }
    return false;
  }

  // Comprueba que el video en directo pueda verse
  public String isLiveVideoPublic(String channel) {
    try {
      // Hacemos la consulta
      PreparedStatement stm = conn.prepareStatement("SELECT filename FROM videos INNER JOIN channels ON channels.id=videos.channel_id WHERE live AND public AND channels.name= ? ;");
      stm.setString(1, channel);
      ResultSet rs = stm.executeQuery();
      if (rs.next()) {
        return rs.getString(1);
      }
    } catch (SQLException ex) {
      System.out.println("-------------> ERROR!!!");
      System.out.println("SQLException: " + ex.getMessage());
      System.out.println("SQLState: " + ex.getSQLState());
      System.out.println("VendorError: " + ex.getErrorCode());
    }
    return null;
  }

  // Incluye una conexion a un video
  public void addVideoView(String filename) {
    try {
      PreparedStatement stm = conn.prepareStatement("UPDATE videos SET views=views+1 WHERE filename= ?;");
      stm.setString(1, filename);
      stm.execute();
    } catch (SQLException ex) {
      System.out.println("-------------> ERROR!!!");
      System.out.println("SQLException: " + ex.getMessage());
      System.out.println("SQLState: " + ex.getSQLState());
      System.out.println("VendorError: " + ex.getErrorCode());
    }
  }

  // Mete estadisticas de un video online
  public void setVideoStats(String filename, IClientBroadcastStreamStatistics stats) {
    try {
      PreparedStatement stm = conn.prepareStatement("UPDATE videos SET viewers= ?, max_viewers=GREATEST(max_viewers, ? ) WHERE filename= ?;");
      stm.setInt(1, stats.getActiveSubscribers());
      stm.setInt(2, stats.getMaxSubscribers());
      stm.setString(3, filename);
      stm.execute();
    } catch (SQLException ex) {
      System.out.println("-------------> ERROR!!!");
      System.out.println("SQLException: " + ex.getMessage());
      System.out.println("SQLState: " + ex.getSQLState());
      System.out.println("VendorError: " + ex.getErrorCode());
    }
  }
} 
