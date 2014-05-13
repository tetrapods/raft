package io.tetrapod.raft;

import java.io.*;
import java.security.SecureRandom;

/**
 * A simple state machine & command for testing purposes.
 * 
 * It maintains a checksum that, when fed random data in commands, will signal any error in application order.
 */
public class TestStateMachine extends StateMachine<TestStateMachine> {
   private SecureRandom random   = new SecureRandom();

   private long         checksum = 0;
   private long         count    = 0;

   public static class Factory implements StateMachine.Factory<TestStateMachine> {
      public TestStateMachine makeStateMachine() {
         return new TestStateMachine();
      }
   }

   public TestCommand makeNewCommand() {
      return new TestCommand(random.nextLong());
   }

   public long getCheckSum() {
      return checksum;
   }

   @Override
   public void saveState(DataOutputStream out) throws IOException {
      out.writeLong(count);
      out.writeLong(checksum);
   }

   @Override
   public void loadState(DataInputStream in) throws IOException {
      count = in.readLong();
      checksum = in.readLong();
   }

   @Override
   public String toString() {
      return String.format("TestStateMachine<%d:%016X>", count, checksum);
   }

   @Override
   public Command<TestStateMachine> makeCommand(int id) {
      switch (id) {
         case TestCommand.COMMAND_ID:
            return new TestCommand();
      }
      return null;
   }

   public static class TestCommand implements Command<TestStateMachine> {
      public static final int COMMAND_ID = 1000;

      private long            val;

      public TestCommand() {}

      public TestCommand(long val) {
         this.val = val;
      }

      public long getVal() {
         return val;
      }
      
      @Override
      public void applyTo(TestStateMachine state) {
         state.checksum ^= (val * ++state.count);
      }

      @Override
      public void write(DataOutputStream out) throws IOException {
         out.writeLong(val);
      }

      public void read(DataInputStream in) throws IOException {
         this.val = in.readLong();
      }

      @Override
      public int getCommandType() {
         return COMMAND_ID;
      }
   }

}
