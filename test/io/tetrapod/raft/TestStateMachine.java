package io.tetrapod.raft;

import java.io.*;
import java.security.SecureRandom;

/**
 * A simple state machine & command for testing purposes.
 * 
 * It maintains a checksum that, when fed random data in commands, will signal any error in application order.
 */
public class TestStateMachine extends StateMachine<TestStateMachine> {
   public SecureRandom random   = new SecureRandom();

   public long         checksum = 0;
   public long         count    = 0;

   public TestCommand makeNewCommand() {
      return new TestCommand(random.nextLong());
   }

   @Override
   public Command<TestStateMachine> makeCommand(int id) {
      switch (id) {
         case 1:
            return new TestCommand();
      }
      return null;
   }

   public class TestCommand implements Command<TestStateMachine> {
      private long val;

      public TestCommand() {}

      public TestCommand(long val) {
         this.val = val;
      }

      @Override
      public void applyTo(TestStateMachine state) {
         checksum ^= (val * ++count);
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
