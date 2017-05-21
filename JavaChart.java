/**
 * Created by Jiao on 2017/4/3.
 */

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.URI;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.text.NumberFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import java.awt.Font;  
import java.awt.GridLayout;  
import javax.swing.JFrame;  
import org.jfree.chart.ChartFactory;  
import org.jfree.chart.ChartPanel;  
import org.jfree.chart.JFreeChart;  
import org.jfree.chart.axis.CategoryAxis;  
import org.jfree.chart.axis.ValueAxis;  
import org.jfree.chart.plot.CategoryPlot;  
import org.jfree.chart.plot.PlotOrientation;  
import org.jfree.data.category.CategoryDataset;  
import org.jfree.data.category.DefaultCategoryDataset;
class BarChart {  
    ChartPanel frame1;  
       private static CategoryDataset getDataSet(double Type1,double Type2,double Type3,double Type4,double Type5,double Type6,double Type7) {  
           DefaultCategoryDataset dataset = new DefaultCategoryDataset();  
           dataset.addValue(Type1, "Type1", "Type1");  
           dataset.addValue(Type2, "Type2", "Type2");  
           dataset.addValue(Type3, "Type3", "Type3");  
           dataset.addValue(Type4, "Type4", "Type4");  
           dataset.addValue(Type5, "Type5", "Type5"); 
		   dataset.addValue(Type6, "Type6", "Type6");  
           dataset.addValue(Type7, "Type7", "Type7");           
return dataset;  
}
public ChartPanel getChartPanel(double Type1,double Type2,double Type3,double Type4,double Type5,double Type6,double Type7){  
   CategoryDataset dataset = getDataSet(Type1,Type2,Type3,Type4,Type5,Type6,Type7);  
        JFreeChart chart = ChartFactory.createBarChart3D(  
                             "Statistical Graph", 
                            "category",
                            "number", 
                            dataset, 
                            PlotOrientation.VERTICAL,  
                            true,          
                            false,         
                            false          
                            );       
        CategoryPlot plot=chart.getCategoryPlot();  
        CategoryAxis domainAxis=plot.getDomainAxis();          
		ValueAxis rangeAxis=plot.getRangeAxis();
         frame1=new ChartPanel(chart,true);        
    return frame1;        
}  
} 
class Hw1{
    public int count ;
    public double avg;
    public double max;
    public Hw1(int count,double avg,double max){
        this.count=count;
        this.avg=avg;
        this.max=max;
    }
    public int hashCode(){
        return (String.valueOf(count)+String.valueOf(avg)+String.valueOf(max)).hashCode();
    }
    public String toString(){
        return String.valueOf(count)+String.valueOf(avg)+String.valueOf(max);
    }
}

public class JavaChart {
    public static void main(String[] args) throws IOException {
double Type1=0;
double Type2=0;
double Type3=0;
double Type4=0;
double Type5=0;
double Type6=0;
double Type7=0;
		Integer group=0;
		String command1="avg(R5)";
        String command2="max(R5)";
        String fileName ="/JVdata/statistics.tbl";
        //hadoop read file
        String file= "hdfs://104.128.92.12:9000//"+fileName;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(file), conf);
        Path path = new Path(file);
        FSDataInputStream in_stream = fs.open(path);
        BufferedReader in = new BufferedReader(new InputStreamReader(in_stream));
        String s;

        Hashtable<String, Hw1> htable = new Hashtable<String,Hw1>();
        while ((s=in.readLine())!=null ) {
            String[] words = s.split("\\|");
            String key = words[group];
            double max_now=Double.valueOf(words[Integer.valueOf(command2.substring(command2.length()-2,command2.length()-1))]);
            double avg_now=Double.valueOf(words[Integer.valueOf(command1.substring(command1.length()-2,command1.length()-1))]);
            if(htable.containsKey(key)){
                Hw1 value=htable.get(key);
                value.count=value.count+1;
       	    	if(max_now>=value.max ){
                        value.max=max_now;
               	}
                value.avg=value.avg+avg_now;
                htable.put(key, value);
            }else {
            	Hw1 hw2 = new Hw1(1,avg_now,max_now);
            	htable.put(key,hw2);
            }

        }
       Iterator<String> iterator0 = htable.keySet().iterator();
        while(iterator0.hasNext()){
            String key = (String)iterator0.next();
            Hw1 value0 = htable.get(key);
            value0.avg=value0.avg/value0.count;

            BigDecimal b   =   new  BigDecimal(value0.avg);
            value0.avg=   b.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
System.out.println("keySet:"+key+" "+"count: "+value0.count+" "+"avg: "+value0.avg+" "+"max: "+value0.max);	   
switch(key){
                case "1":
				    Type1=value0.avg;
                    break;
				case "2":
				    Type2=value0.avg;
                    break;
				case "3":
				    Type3=value0.avg;
                    break;
				case "4":
				    Type4=value0.avg;
                    break;
				case "5":
				    Type5=value0.avg;
                    break;
				case "6":
				    Type6=value0.avg;
                    break;
				case "7":
				    Type7=value0.avg;
                    break;
                default: break;
            }
           
           
        }
        System.out.print("finish it ! The size of htable is ");
        System.out.println(htable.size());

		in.close();
		fs.close();
    JFrame frame=new JFrame("Java Statistics");  
    frame.setLayout(new GridLayout(2,2,10,10));  
    frame.add(new BarChart().getChartPanel(Type1,Type2,Type3,Type4,Type5,Type6,Type7));             
    frame.setBounds(50, 50, 800, 600);  
    frame.setVisible(true); 		
    }
}



