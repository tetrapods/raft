package io.tetrapod.raft;

import java.io.*;
import java.nio.file.Files;

import org.junit.*;
import org.slf4j.*;

/**
 * Unit tests for the raft Log class
 */
public class LogTest {

   public static final Logger logger = LoggerFactory.getLogger(LogTest.class);

   private static File        logDir;

   @BeforeClass
   public static void makeTestDir() throws IOException {
      logDir = Files.createTempDirectory(null).toFile();
   }

   @AfterClass
   public static void deleteTestDir() {
      for (File file : logDir.listFiles()) {
         file.delete();
      }
      logDir.delete();
   }

   @Test
   public void testLog() {
      // create a log
      Log<TestStateMachine> log = new Log<>(logDir, new TestStateMachine());

      // write a bunch of entries
      for (int i = 0; i < 10; i++) {
         log.append(1, log.getState().makeNewCommand());
      }

      Assert.assertEquals(1, log.getFirstEntryIndex());
      Assert.assertEquals(10, log.getLastEntryIndex());

      // test getting all of the entries by index and edges
      Assert.assertNull(log.getEntry(0));
      for (int i = 1; i <= 10; i++) {
         logger.info("{}", i);
         Entry<TestStateMachine> e = log.getEntry(i);
         Assert.assertNotNull(e);
         Assert.assertEquals(i, e.index);
      }
      Assert.assertNull(log.getEntry(11));

      // make sure we can append a higher term
      Assert.assertTrue(log.append(new Entry<TestStateMachine>(2, 11, log.getState().makeNewCommand())));
      Assert.assertNotNull(log.getEntry(11));

      // make sure we cannot append a lower term
      Assert.assertFalse(log.append(new Entry<TestStateMachine>(1, 12, log.getState().makeNewCommand())));
      Assert.assertNull(log.getEntry(12));

   }
}
