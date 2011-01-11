package fm.last.hadoop.tools;

import static com.google.common.collect.Sets.newHashSet;
import static java.lang.System.out;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.Block;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.FSConstants.DatanodeReportType;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.hadoop.net.Node;

public class ReplicationPolicyFixer extends HadoopCommand {
  protected ClientProtocol nameNode;
  protected NetworkTopology cluster;
  protected FileSystem fs;

  public ReplicationPolicyFixer(Configuration conf) throws IOException {
    super(conf);
    this.fs = FileSystem.get(getConf());
  }

  public static int verifyBlockPlacement(LocatedBlock lBlk, short replication, NetworkTopology cluster) {
    try {
      Class<?> replicationTargetChooserClass = Class
          .forName("org.apache.hadoop.hdfs.server.namenode.ReplicationTargetChooser");
      Method verifyBlockPlacementMethod = replicationTargetChooserClass.getDeclaredMethod("verifyBlockPlacement",
          LocatedBlock.class, Short.TYPE, NetworkTopology.class);
      verifyBlockPlacementMethod.setAccessible(true);

      return (Integer) verifyBlockPlacementMethod.invoke(null, lBlk, new Short(replication), cluster);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    } catch (SecurityException e) {
      throw new RuntimeException(e);
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    } catch (IllegalArgumentException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (InvocationTargetException e) {
      Throwable cause = e.getCause();
      if (cause instanceof RuntimeException) {
        cause.printStackTrace();
        throw (RuntimeException) cause;
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public int run(String[] argv) throws IOException {
    nameNode = DFSClient.createNamenode(getConf());
    cluster = new NetworkTopology();
    for (Node node : nameNode.getDatanodeReport(DatanodeReportType.LIVE)) {
      cluster.add(node);
    }

    // Find miss-replicated files
    Set<Path> files = newHashSet();
    FileStatus fileStatus = fs.getFileStatus(new Path("/"));

    findMissReplicatedFiles(fileStatus, files);

    out.println("Got " + files.size() + " files. Done.");

    // out.print("Increasing replication on files. ");
    // out.flush();
    // setReplication(files, targetReplication);
    // out.println("Got " + files.size() + " files. Done.");
    //
    // out.println("Waiting for files to replicate");
    // out.flush();
    // waitForReplication(files, targetReplication);
    // out.println("Done.");
    //
    // out.print("Resetting replication on files. ");
    // out.flush();
    // setReplication(files, defaultReplication);
    // out.println("Done.");

    return 0;
  }

  private int lastPathNameLength = 0;

  private void findMissReplicatedFiles(FileStatus file, Set<Path> missReplicatedFiles) throws IOException {
    Path path = file.getPath();

    if (file.isDir()) {
      FileStatus[] files = fs.listStatus(path);
      if (files == null) {
        return;
      }
      for (FileStatus subFile : files) {
        findMissReplicatedFiles(subFile, missReplicatedFiles);
      }
      return;
    }

    int pathNameLength = path.toUri().getPath().length();
    String padding = StringUtils.repeat(" ", Math.max(0, lastPathNameLength - pathNameLength));
    lastPathNameLength = pathNameLength;
    out.print(path.toUri().getPath() + padding + "\r");
    out.flush();

    LocatedBlocks blocks = nameNode.getBlockLocations(path.toUri().getPath(), 0, file.getLen());
    if (blocks == null) { // the file is deleted
      return;
    }
    if (blocks.isUnderConstruction()) {
      out.println("\nNot checking open file : " + path.toString());
      return;
    }

    for (LocatedBlock lBlk : blocks.getLocatedBlocks()) {
      if (lBlk.isCorrupt()) {
        out.println("\n" + lBlk.toString() + " is corrupt so skipping file : " + path.toString());
        return;
      }

      Block block = lBlk.getBlock();
      DatanodeInfo[] locs = lBlk.getLocations();
      short targetFileReplication = file.getReplication();
      // verify block placement policy
      int missingRacks = verifyBlockPlacement(lBlk, targetFileReplication, cluster);
      if (missingRacks > 0 && locs.length > 0) {
        out.println("\nReplica placement policy is violated for " + block.toString() + " of file " + path.toString()
            + ". Block should be additionally replicated on " + missingRacks + " more rack(s).");
        missReplicatedFiles.add(path);
      }
    }
  }

  void setReplication(List<Path> files, short rep) throws IOException {
    for (Path p : files) {
      fs.setReplication(p, rep);
    }
  }

  /**
   * Wait for all files in waitList to have replication number equal to rep.
   * 
   * @param waitList The files are waited for.
   * @param rep The new replication number.
   * @throws IOException IOException
   */
  void waitForReplication(List<Path> waitList, short rep) throws IOException {
    for (Path f : waitList) {
      boolean printedMessage = false;
      boolean printWarning = false;
      FileStatus status = fs.getFileStatus(f);
      long len = status.getLen();

      for (boolean done = false; !done;) {
        BlockLocation[] locations = fs.getFileBlockLocations(status, 0, len);
        int i = 0;
        for (; i < locations.length && locations[i].getHosts().length == rep; i++) {
          if (!printWarning && locations[i].getHosts().length > rep) {
            System.out
                .println("\nWARNING: the waiting time may be long for " + "DECREASING the number of replication.");
            printWarning = true;
          }
        }
        done = i == locations.length;

        if (!done) {
          if (!printedMessage) {
            System.out.print("Waiting for " + f + " ...");
            System.out.flush();
            printedMessage = true;
          }
          System.out.print(".");
          System.out.flush();
          try {
            Thread.sleep(10000);
          } catch (InterruptedException e) {
          }
        }
      }

      if (printedMessage) {
        System.out.println(" done");
      }
    }
  }

  @Override
  public String getName() {
    return "fixReplication";
  }

  @Override
  public String getDescription() {
    return getShortDescription();
  }

  @Override
  public String getShortDescription() {
    return "Fix block placement policy [DOESN'T WORK YET]";
  }

}
