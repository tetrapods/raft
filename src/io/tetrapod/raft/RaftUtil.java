package io.tetrapod.raft;

import java.io.*;

import org.slf4j.*;

public class RaftUtil {

   public static final Logger logger = LoggerFactory.getLogger(RaftUtil.class);
   
   public static byte[] getFilePart(File file, int offset, int len) {
      byte[] data = new byte[len];
      try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
         raf.seek(offset);
         raf.read(data, 0, len);
         return data;
      } catch (IOException e) {
         logger.error(e.getMessage(), e);
      }
      return null;
   }
}
