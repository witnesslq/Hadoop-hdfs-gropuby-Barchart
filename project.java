
/**
 * Created by Jiao on 2017/4/3.
 */


import java.io.IOException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;



import java.net.URI;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

import java.util.List;
import java.text.NumberFormat;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;


public class project {
    public static void main(String[] args) throws IOException {
		
		//writeback to hbase
		Configuration HBASE_CONFIG = new Configuration();
//		HBASE_CONFIG.set("hbase.zookeeper.quorum", "192.168.0.104");

		String tableName = "HB_MEM_";
		String family="cf";
        HBaseAdmin hBaseAdmin = new HBaseAdmin(HBASE_CONFIG);

        if (hBaseAdmin.tableExists(tableName)) { //check
            hBaseAdmin.disableTable(tableName);
            hBaseAdmin.deleteTable(tableName);
            System.out.println(tableName + " is exist,detele....");
        }

        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
        HColumnDescriptor cf= new HColumnDescriptor(family);
        htd.addFamily(cf);
        hBaseAdmin.createTable(htd);
        hBaseAdmin.close();
		HTable HBasetable = new HTable(HBASE_CONFIG,TableName.valueOf(tableName));
		
		String filePath = "/root/input_2";
		File file=new File(filePath);
		InputStreamReader in_stream = new InputStreamReader(new FileInputStream(file));  
        BufferedReader in = new BufferedReader(in_stream);
        String s;
		int i=0;


        while ((s=in.readLine())!=null ) {
			
            String[] words = s.split(" ");
            String key = words[0];
	    String value=words[1];
            
	    Put put = new Put(key.getBytes());

            put.add(family.getBytes(), "value".getBytes(), value.getBytes());

            System.out.println("Save to Hbase! key:"+key+" "+"value:"+value);
            HBasetable.put(put);
        }
        HBasetable.close();
        System.out.println("put successful!!!");
   
    }
}


