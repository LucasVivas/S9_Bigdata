//=====================================================================
/**
* Squelette minimal d'une application Hadoop
* A exporter dans un jar sans les librairies externes
* A exécuter avec la commande ./hadoop jar NOMDUFICHER.jar ARGUMENTS....
*/
package bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;

public class MonApplication {
	public static class MonProg extends Configured implements Tool {
		public int run(String[] args) throws Exception {
			System.out.println("Hello");
			//CODE DE VOTRE PROGRAMME ICI
			String localInputPath = args[0];
			URI uri = new URI(args[1]);
			uri = uri.normalize();
			Configuration conf = getConf();
			FileSystem fs = FileSystem.get(uri, conf, "lvivas");
			Path outputPath = new Path(uri.getPath());
			OutputStream os = fs.create(outputPath);
			InputStream is = new BufferedInputStream(new FileInputStream(localInputPath));
			IOUtils.copyBytes(is, os, conf);
			os.close();
			is.close();
			return 0;
		}
	}
	public static void main( String[] args ) throws Exception {
		int returnCode = ToolRunner.run(new MonApplication.MonProg(), args);
		System.exit(returnCode);
	}
}
//=====================================================================

