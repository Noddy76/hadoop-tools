package fm.last.hadoop.tools;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;

public abstract class HadoopCommand extends Configured {
  public HadoopCommand(Configuration conf) {
    setConf(conf);
  }

  public abstract int run(String[] argv) throws IOException;

  public abstract String getName();

  public abstract String getDescription();

  public abstract String getShortDescription();
}
