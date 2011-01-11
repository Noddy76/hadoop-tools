package fm.last.hadoop.tools;

import static com.google.common.collect.Lists.newArrayList;
import static java.lang.System.out;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;

import com.google.common.base.Joiner;

public class BlockFinder extends HadoopCommand {
  protected FileSystem fs;

  public BlockFinder(Configuration conf) throws IOException {
    super(conf);
    this.fs = FileSystem.get(getConf());
  }

  @Override
  public int run(String[] argv) throws IOException {
    StringBuilder b = new StringBuilder();

    ClientProtocol namenode = DFSClient.createNamenode(getConf());
    for (String fileName : argv) {
      FileStatus[] fileStatuses = fs.globStatus(new Path(fileName));
      for (FileStatus fileStatus : fileStatuses) {
        if (!fileStatus.isDir()) {
          out.println("FILE: " + fileStatus.getPath().toString());

          String path = fileStatus.getPath().toUri().getPath();
          LocatedBlocks blocks = namenode.getBlockLocations(path, 0, fileStatus.getLen());

          for (LocatedBlock block : blocks.getLocatedBlocks()) {
            b.setLength(0);
            b.append(block.getBlock());
            b.append(" - ");

            List<String> nodes = newArrayList();
            for (DatanodeInfo datanodeInfo : block.getLocations()) {
              nodes.add(datanodeInfo.name);
            }
            b.append(Joiner.on(", ").join(nodes));
            out.println(b.toString());
          }

        }
        out.println();
      }
    }
    return 0;
  }

  @Override
  public String getName() {
    return "blockFinder";
  }

  @Override
  public String getDescription() {
    return getShortDescription();
  }

  @Override
  public String getShortDescription() {
    return "Display the block names and locations of the files specified";
  }

}
