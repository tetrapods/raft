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

   public static byte[] toBytes(long value) {
      return new byte[] { (byte) (value >>> 56), (byte) (value >>> 48), (byte) (value >>> 40), (byte) (value >>> 32),
            (byte) (value >>> 24), (byte) (value >>> 16), (byte) (value >>> 8), (byte) value };
   }

   public static long fromBytes(byte[] data) {
      return (((long) data[0] << 56) + ((long) (data[1] & 255) << 48) + ((long) (data[2] & 255) << 40) + ((long) (data[3] & 255) << 32)
            + ((long) (data[4] & 255) << 24) + ((data[5] & 255) << 16) + ((data[6] & 255) << 8) + ((data[7] & 255) << 0));
   }
}
