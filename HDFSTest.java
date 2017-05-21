import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;


public class HDFSTest {
public static void main(String[] args) throws IOException{
String filename="/JVdata/helloword.tbl";
String file= "hdfs://104.128.92.12:9000/"+filename;
Configuration conf = new Configuration();
FileSystem fs = FileSystem.get(URI.create(file), conf);
Path path = new Path(file);
FSDataInputStream in_stream = fs.open(path);
BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
String s;
while ((s=in.readLine())!=null) {
System.out.println(s);
}
in.close();
fs.close();
}
}
