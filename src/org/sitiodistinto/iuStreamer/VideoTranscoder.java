package org.sitiodistinto.iuStreamer;

import java.lang.System;
import java.io.File;

import org.slf4j.Logger;

import org.red5.logging.Red5LoggerFactory;
import org.red5.server.api.IContext;
import org.red5.server.api.IScope;
import org.red5.server.api.stream.IBroadcastStream;
import org.red5.server.stream.BroadcastScope;
import org.red5.server.stream.IBroadcastScope;
import org.red5.server.stream.IProviderService;

import com.xuggle.xuggler.ICodec;
import com.xuggle.xuggler.IVideoPicture;
import com.xuggle.xuggler.video.IConverter;
import com.xuggle.xuggler.video.ConverterFactory;
import com.xuggle.xuggler.IPixelFormat;
import com.xuggle.xuggler.ISimpleMediaFile;
import com.xuggle.xuggler.SimpleMediaFile;

import com.xuggle.ferry.IBuffer;
import com.xuggle.red5.VideoPictureListener;
import com.xuggle.red5.IVideoPictureListener;
import com.xuggle.red5.Transcoder;
import com.xuggle.red5.io.BroadcastStream;

import java.awt.image.BufferedImage;
import javax.imageio.ImageIO;


/**
 * This class takes an IBroadcastStream and modifies each video picture
 * to mirror the right half in the left half, and grayscale the left half.
 */
public class VideoTranscoder implements IVideoPictureListener {
  final private Logger log = Red5LoggerFactory.getLogger(this.getClass());
  private IBroadcastStream stream;
  private BroadcastStream outputStream;
  private IScope scope;
  private Transcoder transcoder;

  public VideoTranscoder(IBroadcastStream stream, IScope scope) {
    this.stream = stream;
    this.scope = scope;
  }

  // Override methods from IVideoPictureListener
  public IVideoPicture preEncode(IVideoPicture picture) {
    long pos = picture.getPts()/1000000;
    if (picture.isKeyFrame() && pos > 2) {
      String filename = stream.getSaveFilename() + ".png";
       if (!(new File(filename)).exists()) {
         //IConverter converter = ConverterFactory.createConverter(stream.getName(),picture);
         BufferedImage image = new BufferedImage(picture.getWidth(), picture.getHeight(),BufferedImage.TYPE_3BYTE_BGR);
         IConverter converter = ConverterFactory.createConverter(image, picture.getPixelType());
         //IConverter converter = ConverterFactory.createConverter(image, IPixelFormat.Type.BGR24);
         if (converter != null) {
           writeThumbnail(filename,converter.toImage(picture));
         }
       }
     }
     return picture;
  }
  public IVideoPicture postDecode(IVideoPicture aObject) { return aObject; }
  public IVideoPicture postResample(IVideoPicture aObject) { return aObject; }
  public IVideoPicture preResample(IVideoPicture aObject) { return aObject; }

  synchronized public void startTranscodingStream() {
    String outputName = "transcode_" + stream.getPublishedName();
    outputStream = new BroadcastStream(outputName);
    outputStream.setPublishedName(outputName);
    outputStream.setScope(scope);
    IContext context = scope.getContext();

    IProviderService providerService = (IProviderService) context.getBean(IProviderService.BEAN_NAME);
    if (providerService.registerBroadcastStream(scope, outputName, outputStream)) {
      IBroadcastScope bsScope = (BroadcastScope) providerService.getLiveProviderInput(scope, outputName, true);
      bsScope.setAttribute(IBroadcastScope.STREAM_ATTRIBUTE, outputStream);
    } else {
      System.out.println("Got a fatal error; could not register broadcast stream");
      throw new RuntimeException("fooey!");
    }
    outputStream.start();

    /**
     * Now let's give aaffmpeg-red5 some information about what we want to transcode as. 
     */
    ISimpleMediaFile outputStreamInfo = new SimpleMediaFile();
    outputStreamInfo.setHasAudio(true);
    outputStreamInfo.setAudioBitRate(32000);
    outputStreamInfo.setAudioChannels(1);
    outputStreamInfo.setAudioSampleRate(22050);
    outputStreamInfo.setAudioCodec(ICodec.ID.CODEC_ID_MP3);
    outputStreamInfo.setHasVideo(true);
    // Unfortunately the Trans-coder needs to know the width and height
    // you want to output as; even if you don't know yet.
    outputStreamInfo.setVideoWidth(320);
    outputStreamInfo.setVideoHeight(240);
    outputStreamInfo.setVideoBitRate(320000);
    outputStreamInfo.setVideoCodec(ICodec.ID.CODEC_ID_FLV1);
    outputStreamInfo.setVideoGlobalQuality(0);

    this.transcoder = new Transcoder(stream, outputStream, outputStreamInfo, null, null, this);
    Thread transcoderThread = new Thread(this.transcoder);
    transcoderThread.setDaemon(true);
    transcoderThread.start();
  }

  synchronized public void stopTranscodingStream() {
    String inputName = stream.getPublishedName(); 
    if (transcoder != null) {
      transcoder.stop();
    }
    if (outputStream != null) {
      outputStream.stop();
    }
  }

  // Writes thumbnail to disk
  private boolean writeThumbnail(String outputFilename, BufferedImage image) {
    try {
      ImageIO.write(image, "png", new File(outputFilename));
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }
}
