/**
 * Copyright 2011 Last.fm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package fm.last.hadoop.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Tools extends Configured implements Tool {

  private final List<HadoopCommand> commands = new ArrayList<HadoopCommand>();

  public void initialiseCommands() throws IOException {
    commands.add(new BlockFinder(getConf()));
    commands.add(new ReplicationPolicyFixer(getConf()));
  }

  private void printUsage(String cmd) {
    String prefix = "Usage: java " + this.getClass().getSimpleName();

    for (HadoopCommand command : commands) {
      if (command.getName().equals(cmd)) {
        System.err.println(prefix + " [-" + command.getName() + "]\n" + command.getDescription());
        return;
      }
    }

    System.err.println(prefix);
    for (HadoopCommand command : commands) {
      System.err.println("           [-" + command.getName() + "] " + command.getShortDescription());
    }
    System.err.println();

    ToolRunner.printGenericCommandUsage(System.err);
  }

  @Override
  public int run(String[] argv) throws Exception {
    getConf().setQuietMode(true);

    initialiseCommands();

    if (argv.length < 1) {
      printUsage("");
      return -1;
    }

    int exitCode = -1;
    String cmd = argv[0];
    argv = Arrays.copyOfRange(argv, 1, argv.length);

    HadoopCommand command = null;

    if (cmd.length() > 1 && '-' == cmd.charAt(0)) {
      cmd = cmd.substring(1);
      for (HadoopCommand possibleCommand : commands) {
        if (cmd.equals(possibleCommand.getName())) {
          command = possibleCommand;
          break;
        }
      }
    }

    if (command == null) {
      exitCode = -1;
      System.err.println(cmd + ": Unknown command");
      printUsage("");
    } else {
      exitCode = command.run(argv);
    }

    return exitCode;
  }

  public void close() throws IOException {
    FileSystem fs = FileSystem.get(getConf());
    if (fs != null) {
      fs.close();
      fs = null;
    }
  }

  /**
   * main() has some simple utility methods
   */
  public static void main(String argv[]) throws Exception {
    Tools shell = new Tools();
    int res;
    try {
      res = ToolRunner.run(shell, argv);
    } finally {
      shell.close();
    }
    System.exit(res);
  }
}
