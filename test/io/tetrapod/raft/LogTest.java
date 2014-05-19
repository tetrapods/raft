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
   public void testLog() throws Exception {
      TestStateMachine state = new TestStateMachine();
      Config config = new Config().setLogDir(logDir);

      // create a log
      Log<TestStateMachine> log = new Log<>(config, state);

      // write a bunch of entries
      for (int i = 0; i < 10; i++) {
         log.append(1, state.makeNewCommand());
      }

      Assert.assertEquals(1, log.getFirstIndex());
      Assert.assertEquals(10, log.getLastIndex());

      // test getting all of the entries by index and edges
      Assert.assertNull(log.getEntry(0));
      for (int i = 1; i <= 10; i++) {
         Entry<TestStateMachine> e = log.getEntry(i);
         Assert.assertNotNull(e);
         Assert.assertEquals(i, e.index);
      }
      Assert.assertNull(log.getEntry(11));

      // make sure we can append a higher term
      Assert.assertTrue(log.append(new Entry<TestStateMachine>(2, 11, state.makeNewCommand())));
      Assert.assertNotNull(log.getEntry(11));

      // make sure we cannot append a lower term
      Assert.assertFalse(log.append(new Entry<TestStateMachine>(1, 12, state.makeNewCommand())));
      Assert.assertNull(log.getEntry(12));

      log.setCommitIndex(log.getLastIndex());
      while (log.getStateMachine().getIndex() < log.getLastIndex()) {
         sleep(100);
      }
      long checksum = state.getCheckSum();
      logger.info("State = {}", state);
      log.stop();

      sleep(1000);

      // create a new log

      state = new TestStateMachine();
      log = new Log<>(config, state);
      Assert.assertEquals(1, log.getFirstIndex());
      Assert.assertEquals(11, log.getLastIndex());

      log.setCommitIndex(log.getLastIndex());
      while (log.getStateMachine().getIndex() < log.getLastIndex()) {
         sleep(100);
      }
      Assert.assertEquals(checksum, state.getCheckSum());
      logger.info("State = {}", state);

      // write a bunch of entries
      for (int i = 0; i < 10; i++) {
         log.append(3, state.makeNewCommand());
      }
      Assert.assertEquals(1, log.getFirstIndex());
      Assert.assertEquals(21, log.getLastIndex());

      //      File snapFile = new File(logDir, "raft.snapshot");
      //      state.writeSnapshot(snapFile);
      //      log.compact(state.getIndex(), 0);
      //      Assert.assertEquals(12, log.getFirstIndex());
      //      Assert.assertEquals(21, log.getLastIndex());
      //      log.setCommitIndex(log.getLastIndex());
      //      log.stop();
      //      sleep(1000);
      //
      //      state = new TestStateMachine();
      //      state.readSnapshot(snapFile);      
      //      Assert.assertEquals(checksum, state.getCheckSum());
      //      Assert.assertEquals(state.getIndex(), 11);
      //      log = new Log<>(logDir, state);
      //
      //      Assert.assertEquals(12, log.getFirstIndex());
      //      Assert.assertEquals(21, log.getLastIndex());

   }

   private void sleep(int millis) {
      try {
         Thread.sleep(millis);
      } catch (InterruptedException e) {}
   }
}
