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

   public TestCommand makeNewCommand() {
      return new TestCommand(random.nextLong());
   }

   public long getCheckSum() {
      return checksum;
   }

   @Override
   public void reset() {
      super.reset();
      checksum = 0;
      count = 0;
   }

   @Override
   public String toString() {
      return String.format("TestStateMachine<%d:%016X>", count, checksum);
   }

   @Override
   public Command<TestStateMachine> makeCommand(int id) {
      switch (id) {
         case 1:
            return new TestCommand();
      }
      return null;
   }

   public static class TestCommand implements Command<TestStateMachine> {
      private long val;

      public TestCommand() {}

      public TestCommand(long val) {
         this.val = val;
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
         return 1;
      }
   }

}
