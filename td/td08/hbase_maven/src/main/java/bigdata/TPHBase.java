//=====================================================================
/**
 * Squelette minimal d'une application HBase 0.99.1
 * A exporter dans un jar sans les librairies externes
 * Il faut initialiser la variable d'environement HADOOP_CLASSPATH
 * Il faut utiliser la commande hbase 
 * A exécuter avec la commande ./hadoop jar NOMDUFICHER.jar ARGUMENTS....
 */
package bigdata;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class TPHBase {

	public static class HBaseProg extends Configured implements Tool {
		private static final byte[] LOCFAMILY = Bytes.toBytes("loc");
		private static final byte[] NAMEFAMILY = Bytes.toBytes("name");
		private static final byte[] REGFAMILY = Bytes.toBytes("reg");
		private static final byte[] MEASUREFAMILY = Bytes.toBytes("measure");
		private static final byte[] ROW    = Bytes.toBytes("BBB");
		private static final byte[] TABLE_NAME = Bytes.toBytes("micheldobrazil");

		public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
			if (admin.tableExists(table.getTableName())) {
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);
		}

		public static void createTable(Connection connect) {
			try {
				final Admin admin = connect.getAdmin(); 
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
				HColumnDescriptor famLoc = new HColumnDescriptor(LOCFAMILY);
				HColumnDescriptor famName = new HColumnDescriptor(NAMEFAMILY);
				HColumnDescriptor famReg = new HColumnDescriptor(REGFAMILY);
				HColumnDescriptor famMeasure = new HColumnDescriptor(MEASUREFAMILY);

				famLoc.setMaxVersions(5);
				famLoc.setInMemory(true);

				tableDescriptor.addFamily(famLoc);
				tableDescriptor.addFamily(famName);
				tableDescriptor.addFamily(famReg);
				tableDescriptor.addFamily(famMeasure);
				createOrOverwrite(admin, tableDescriptor);
				admin.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}

		public int run(String[] args) throws IOException {
			Connection connection = ConnectionFactory.createConnection(getConf());
			createTable(connection);
			Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
			Put put = new Put(Bytes.toBytes("KEY"));
			table.put(put);
			return 0;
		}

	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(HBaseConfiguration.create(), new TPHBase.HBaseProg(), args);
		System.exit(exitCode);
	}
}

