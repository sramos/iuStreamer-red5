package org.sitiodistinto.iuStreamer;

import org.red5.server.api.IScope; 
import org.red5.server.api.stream.IStreamPlaybackSecurity;
            
public class PlaybackSecurity implements IStreamPlaybackSecurity { 

  private MySqlDatabase database;

  public boolean isPlaybackAllowed(IScope scope, String name, int start, int length, boolean flushPlaylist) {
    boolean status = false;

    if (name.equals("live")) {
      String filename = database.isLiveVideoPublic(scope.getContextPath().substring(1));
      if ( filename != null ) {
        name = filename;
        status = true;
      }
    } else {
      status = database.isVODVideoPublic(name);
    }

    if (status) {
      //System.out.println("Nos piden reproducir el video: " + name);
      database.addVideoView(name);
    } else {
      System.out.println("Nos piden reproducir un video que es privado: " + name);
    }

    return status; 
  }

  public void setDatabase(MySqlDatabase db) {
    database = db;
  }

} 
