package org.sitiodistinto.iuStreamer;

import org.red5.server.adapter.ApplicationAdapter;
import org.red5.server.api.IConnection;
import org.red5.server.api.IScope;
import org.red5.server.api.IClient;
import org.red5.server.api.Red5;
import org.red5.server.api.stream.IServerStream;
import org.red5.server.api.stream.IClientBroadcastStream;
import org.red5.server.api.stream.IBroadcastStream;
import org.red5.server.stream.ClientBroadcastStream;
import org.red5.server.api.stream.ISubscriberStream;
import org.red5.server.api.scheduling.IScheduledJob;
import org.red5.server.api.scheduling.ISchedulingService;

//import com.xuggle.xuggler.IContainer;

import java.lang.System;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import org.slf4j.Logger;

public class Application extends ApplicationAdapter implements IScheduledJob {

  private IScope appScope;
  private IServerStream serverStream;

  private String dbname = "";
  private String dbuser = "";
  private String dbpass = "";
  private MySqlDatabase database;
  private Map<String, VideoTranscoder> mTranscoders = new HashMap<String, VideoTranscoder>();

  public void init() {
    //log.debug("iuStreamer has started.  Be afraid: {}", this.getClass().getName());
    System.out.println("iuStreamer has started.  Be afraid: " + this.getClass().getName());

    // This forces us to load the Xuggler shared library.  It's only
    // done because if we're debugging C++ from Java (please don't ask)
    // this allows a breakpoint
   //IContainer.make();
  }

	/** {@inheritDoc} */
    @Override
	public boolean appStart(IScope app) {
          // Inicialize objects 
          database = new MySqlDatabase(dbname,dbuser,dbpass); 
          TokenPublishSecurity sec_w = new TokenPublishSecurity();
          PlaybackSecurity sec_r = new PlaybackSecurity();

          // Populate database parameters 
          sec_w.setDatabase(database);
          sec_r.setDatabase(database);

          // Register objects 
          registerStreamPublishSecurity(sec_w);
          registerStreamPlaybackSecurity(sec_r);
          addScheduledJob(5000, this);

          // Finish starting app 
	  super.appStart(app);
	  log.info("iuStreamer appStart");
	  System.out.println("iuStreamer appStart");    	
	  appScope = app;
	  return true;
	}

	/** {@inheritDoc} */
    @Override
	public boolean appConnect(IConnection conn, Object[] params) {
		boolean status = true;
		log.info("iuStreamer appConnect");
		return super.appConnect(conn, params);
	}

	/** {@inheritDoc} */
    @Override
	public void appDisconnect(IConnection conn) {
		log.info("iuStreamer appDisconnect");
		if (appScope == conn.getScope() && serverStream != null) {
			serverStream.close();
		}
		super.appDisconnect(conn);
	}

    // Callback for broadcast start 
    public void streamBroadcastStart(IBroadcastStream stream) {
      if(!(stream instanceof IClientBroadcastStream)) {
	System.out.println("No es instancia de IClientBroadcastStream");
        return;
      }
      // Le cambiamos el nombre al stream publicado segun el usuario que emite (parece que esto no funciona...)
      //stream.setPublishedName("live");
      System.out.println( "streamBroadcastStart: Broadcast started, name: " + stream.getPublishedName() + "; internal name: " + stream.getName() + " by client #" +((IClientBroadcastStream)stream).getConnection().getClient().getId() + " in scope " + stream.getScope() );
      try {
        // Saves a copy of stream 
        stream.saveAs(stream.getName(), false);
        // Write video to disk and store it into DB
        database.startRecord(stream.getScope().getContextPath().substring(1), stream.getScope().getContextPath().substring(1) + "/" + stream.getName());
      } catch (Exception e) {
        e.printStackTrace();
      }
    } 

    // Callback for publish start
      @Override
    public void streamPublishStart(IBroadcastStream stream) {
      //log.debug("streamPublishStart: {}; {}", stream, stream.getPublishedName());
      super.streamPublishStart(stream);
      // Create and store a new transcoding class
      if (!stream.getPublishedName().startsWith("transcode_")) {
        VideoTranscoder transcoder = new VideoTranscoder(stream, Red5.getConnectionLocal().getScope());
        transcoder.startTranscodingStream();
        mTranscoders.put(stream.getName(), transcoder );
      }
    }

    // Callback for broadcast close
    public void streamBroadcastClose(IBroadcastStream stream) {
      System.out.println( "streamBroadcastStop: Broadcast stoped, name: " + stream.getPublishedName() + "; internal name: " + stream.getName() + " by client #" +((IClientBroadcastStream)stream).getConnection().getClient().getId() );
      // Stops transcoding (if any)
      if (!stream.getPublishedName().startsWith("transcode_")) {
        VideoTranscoder transcoder = mTranscoders.get(stream.getName());
        if (transcoder != null) {
          transcoder.stopTranscodingStream();
          mTranscoders.remove(stream.getName());
        }
      }
      // Write stats into DB
      database.setVideoStats(stream.getScope().getContextPath().substring(1) + "/" + stream.getName(),((IClientBroadcastStream)stream).getStatistics());
      // Stream is no longer live
      database.stopRecord(stream.getScope().getContextPath().substring(1) + "/" + stream.getName(), ((ClientBroadcastStream) stream).getCurrentTimestamp()); 
    }

    // Callback for client connection
    public void streamSubscriberStart ( ISubscriberStream stream ) {
      //System.out.println("Alguien ha pedido hacer streaming del video " + stream.getScope().getContextPath() + " - " + stream.getName());
      //database.addViewer(stream.getName());
    }

    // Callback for client disconnection 
    public void streamSubscriberClose ( ISubscriberStream stream ) {
      //System.out.println("Se ha parado el streaming del video " + stream.getName());
    }

    // This method is called by scheduller
    public void execute (ISchedulingService isservice) {
      // Loop into all live broadcasts to store statistics 
      try {
        Iterator scopes = appScope.getScopeNames();
        while ( scopes.hasNext() ) {
          IScope current_scope = appScope.getScope( ((String)scopes.next()).substring(1) );
          Iterator it = getBroadcastStreamNames(current_scope).iterator();
          while (it.hasNext()) {
            String nombre = (String)it.next();
            if (nombre != null) {
              IBroadcastStream stream = getBroadcastStream(current_scope,nombre);
              // Write stats into DB
              database.setVideoStats(stream.getScope().getContextPath().substring(1) + "/" + stream.getName(),((IClientBroadcastStream)stream).getStatistics());
            }
          }
        }
      } catch (Exception e) {
        System.out.println("Error: " + e);
      }
    }

    public void setDbname(String obj) { dbname = obj; }
    public void setDbuser(String obj) { dbuser = obj; }
    public void setDbpass(String obj) { dbpass = obj; }
}

