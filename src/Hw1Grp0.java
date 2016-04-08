/**
 * Created by xd on 2016/4/5.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Hashtable;

/**

 write by xudong
 2016.4.5
 **/

public class Hw1Grp0 {

    private Hashtable<String, String> joinData = new Hashtable<String, String>();


    public static void writeInHbase( String tablename) throws IOException {
//        HBaseConfiguration configuration = new HBaseConfiguration();

        Logger.getRootLogger().setLevel(Level.WARN);
        Configuration configuration = HBaseConfiguration.create();
        HBaseAdmin admin = new HBaseAdmin(configuration);
        if (admin.tableExists(tablename)) {  //如果表已经存在
            System.out.println("table exits, Trying recreate table!");
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
        }
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tablename)); //row
        HColumnDescriptor col = new HColumnDescriptor("familar1"); //列族
        HColumnDescriptor col1 = new HColumnDescriptor("familar2");

        htd.addFamily(col); //创建列族
        htd.addFamily(col1);


        admin.createTable(htd); //创建表
        System.out.println("Create new table: " + tablename);
        admin.close();

        HTable table = new HTable(configuration,tablename);
        //由文件中读取 hashjoined.tbl
        String path = "./hashjoined.tbl";
        File file = new File(path);
        InputStream in_stream = new FileInputStream(file);
        BufferedReader in = new BufferedReader( (new InputStreamReader(in_stream)));
        //写入HBase
        String[] tokens ;
        String line;

        try {
            while ((line = in.readLine()) != null){
                tokens = line.split("\\|");
                Put put =  new Put(tokens[0].getBytes());
                for(int i = 1;i < 9;i++)
                {// 加进第一个表里的列 tokens1-8
                    put.add("familar1".getBytes(), ("qualifier" + Integer.toString(i)).getBytes(), tokens[i].getBytes());
                }

                for (int i = 9;i < 16;i++)
                {// 加进第二个表里的列 tokens9-15
                    put.add("familar2".getBytes(),("qualifier" + Integer.toString(i)).getBytes(),tokens[i].getBytes());
                }
                table.put(put);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            in.close();
            table.close();

        }
    }



    public  void hashJoin() throws IOException, URISyntaxException {
        String file = "hdfs://localhost:9000/hw1/part.tbl";
        String file2 = "hdfs://localhost:9000/hw1/customer.tbl";

        //read part.tbl and customer.tbl from hdfs.

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file),conf);
        FileSystem fs1 = FileSystem.get(URI.create(file2),conf);

        Path path = new Path(file);
        Path path1 = new Path(file2);

        FSDataInputStream in_stream = fs.open(path);
        BufferedReader in = new BufferedReader((new InputStreamReader(in_stream)));

        FSDataInputStream in_stream1 = fs1.open(path1);
        BufferedReader in1 = new BufferedReader((new InputStreamReader(in_stream1)));

        String[] tokens;
        String line;

        //将part.tbl表做成 第一列为key，剩下列为value的hashtable.
        try {
            while((line=in.readLine()) != null){
                tokens = line.split("\\|",2);
                joinData.put(tokens[0],tokens[1]);

            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            in.close();
        }

        //按行读取customer.tbl表，每读一行将customer 的id分开，变为 tokens1[0],tokens2[1],分别带入joinData中求value，如有
        //则将其与tokens[0]tokens[1]一块输出。
        // 并写入hashjoined.tbl 文件
        String[] tokens1;
        String line1;
        // 将要写入join结果的文件
        File joinedfile = new File("hashjoined.tbl");
        // 如果不存在此文件，则创建一个
        if(!joinedfile.exists()){
            joinedfile.createNewFile();
        }
        FileWriter fileWriter = new FileWriter(joinedfile.getName(),true);
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        try {
            while ((line1 = in1.readLine()) != null) {
                tokens1 = line1.split("\\|", 2);
                String value = joinData.get(tokens1[0]);
                if (value != null) {
                    String joinedline = tokens1[0] + "|" + value + tokens1[1] + System.getProperty("line.separator");
                    bufferedWriter.write(joinedline);

                }
            }
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            in1.close();
            bufferedWriter.close();
        }
    }


    public static void main(String[] args) throws IOException, URISyntaxException {

        Hw1Grp0 hw1 = new Hw1Grp0();
        //对两表数据进行hashjoin
        hw1.hashJoin();
        //创建一个hbase 表名hashJoinedTable  将join后的表写入数据库

        hw1.writeInHbase("hashJoinedTable");


    }

}

