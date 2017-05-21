# Hadoop-hdfs-gropuby-Barchart
java 从hdfs中读取数据，进行groupby 得到avg、count、max,然后用柱状图显示统计结果

## java 从hadoop hdfs读取文件 进行groupby并显示为条形图

- **题意：从文件、网络或者数据库中读取数据（格式自定、数据自定），显示统计结果（包括图形两种以上），用户界面自定**

有兴趣使用的，请点[源代码与数据下载链接](http://download.csdn.net/download/xmo_jiao/9847929)


-------------------

## 1 读取数据
### 1.1准备数据
	此数据为TPCH基准测试集中lineitem.tdl文件中前25行
示例：第一行如下 
1|1552|93|1|17|24710.35|0.04|0.02|N|O|1996-03-13|1996-02-12|1996-03-22|DELIVERIN PERSON|TRUCK|egular courts above the|

**其中有15列，分别以“|”隔开**
 
- 第0列：1
- 第1列：1552
- 第2列：93
- 第n列：…

全部数据截图如下：
![数据截图](http://img.blog.csdn.net/20170521010857745?watermark/2/text/aHR0cDovL2Jsb2cuY3Nkbi5uZXQvWG1vX2ppYW8=/font/5a6L5L2T/fontsize/400/fill/I0JBQkFCMA==/dissolve/70/gravity/SouthEast)
### 1.2 将数据存入HDFS:
	文件系统：HDFS全名为hadoop Distributed File System，是google File system的开源实现，是一种基于java的应用层文件系统，与hadoop捆绑在一起。HDFS设计成能可靠地在集群中大量机器之间存储大量的文件，它以块序列的形式存储文件。
	在hadoop集群开启的情况下，使用以下命令将数据存储在hadoop hdfs文件系统的JVdata文件夹中。


```
#hadoop fs –copyFromLocal statistics.tbl ./JVdata
```

### 1.3 读取数据
使用hdfs的API读取数据流，in.readline为按行读取数据。
“hdfs://localhost:9000/文件路径"为hadoop中地址，需要与${HADOOP_HOME}/etc/Hadoop/core-site.xml设置文件中保持一致

```
        <property>
             <name>fs.defaultFS</name>
             <value>hdfs://104.128.92.12:9000</value>
        </property>
```

```
public class HDFSTest {
public static void main(String[] args) throws IOException, URISyntaxException{
String file= “hdfs://localhost:9000/文件路径";
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

```




## 2.统计数据
### 2.1使用hashtable键值对方法统计数据
	本工程分别对分组数据进行count,求平均avg,求最大值max处理,所以hashtable中键为分组统计的关键字，值有三个，所以此处自定义一个class,便于构建一键多值的hashtable.
	

```
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
```



### 2.2 groupby分组处理数据
	分别对三种目标结果进行计算统计，依次处理每行的数据，将行累加或者求最大。其中word为字符串类型的数组，将元数据用“|”分割，使用下标法对数组取值。
初始化

```
Hw1 hw2 = new Hw1(1,avg_now,max_now);
htable.put(key,hw2);
```


将键为key,值为hw2的值put到hashtable中
其中avg_now，max_now为第一次出现key时的相应列的值，

```
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

```
       
## 3.显示数据
### 3.1打印数据
	使用迭代器对hashtable中的值进行遍历，使用iterator0.hasNext()判断迭代是否完成，next()为迭代器遍历下一个关键字的方法。htable.get(key)为得key值对应的value,对value.avg平均数取小数点后两位
System.out.println(“ ”)打印出统计结果

```

Iterator<String> iterator0 = htable.keySet().iterator();
while(iterator0.hasNext()){
            String key = (String)iterator0.next();
            Hw1 value0 = htable.get(key);
            value0.avg=value0.avg/value0.count;

            BigDecimal b   =   new  BigDecimal(value0.avg);
            value0.avg=   b.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
          System.out.println("keySet:"+key+" "+value0.count+" "+value0.avg+" "+value0.max);
}
```

### 3.2 柱状图显示数据
	使用switch-case语句，将hashtable中统计的平均数据，传给相应变量，然后使用第三方包实现柱状图的绘制
#### 3.2.1 收集数据集
	将上述统计的来的数据使用CategoryDataset包装在一起，返回dataset便于画图使用。
	

```
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
```


输出：
keySet:6 count: 1 avg: 48040.43 max: 48040.43
keySet:5 count: 3 avg: 36098.98 max: 63818.5
keySet:4 count: 1 avg: 53456.4 max: 53456.4
keySet:3 count: 6 avg: 36405.1 max: 53468.31
keySet:2 count: 1 avg: 36596.28 max: 36596.28
keySet:1 count: 6 avg: 30122.44 max: 56688.12
keySet:7 count: 7 avg: 40209.09 max: 85051.24
finish it ! The size of htable is 7
 ![这里写图片描述](http://img.blog.csdn.net/20170521010955528?watermark/2/text/aHR0cDovL2Jsb2cuY3Nkbi5uZXQvWG1vX2ppYW8=/font/5a6L5L2T/fontsize/400/fill/I0JBQkFCMA==/dissolve/70/gravity/SouthEast)
#### 3.2.2 设置横坐标，纵坐标和label,以及表头

```
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
```


输出：

 ![这里写图片描述](http://img.blog.csdn.net/20170521011034967?watermark/2/text/aHR0cDovL2Jsb2cuY3Nkbi5uZXQvWG1vX2ppYW8=/font/5a6L5L2T/fontsize/400/fill/I0JBQkFCMA==/dissolve/70/gravity/SouthEast)


## 4 编译运行：
	/root/jars/jfreechart-1.0.13/lib为外部包路径，JavaChart.java为主函数，会产生多个class,但是JavaChart为主要class. -Djava.ext.dirs为加载第三方包，此处为包的路径

```
#vim JavaChart.java
#javac -Djava.ext.dirs=./jfreechart-1.0.13/lib JavaChart.java
#java -Djava.ext.dirs=./jfreechart-1.0.13/lib JavaChart


```


  
#### 目录

[TOC]


### UML 图:

```sequence
hadoop hdfs->java编辑器: 读取数据
Note right of java编辑器: groupby 得到count、avg、max
java编辑器-->文字和图形显示（Xming）: 
```

