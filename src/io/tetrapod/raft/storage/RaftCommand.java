package io.tetrapod.raft.storage;

import java.lang.annotation.*;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface RaftCommand {
   int id();
}
