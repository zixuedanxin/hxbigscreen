package com.dxhy.mr.solr;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.common.SolrInputDocument;

import com.alibaba.fastjson.JSONObject;

//com.dxhy.mr.solr.HBaseIndexerMain
//5mins, 38sec - 805483
public class HBaseIndexerMain {

	public static Logger log = Logger.getLogger(HBaseIndexerMain.class);

    public static class HBaseIndexerMapper extends TableMapper<Text, MapWritable> {
	    
	    @Override  
	    public void map(ImmutableBytesWritable row, Result result, Context context)  
	            throws IOException {  

	        //获取rowKey
	        String rowKey = Bytes.toString(result.getRow());
	        
	        String data = Bytes.toString(result.getValue(Bytes.toBytes("Info"), Bytes.toBytes("Data")));
	        if(data.getClass().equals(String.class)){
	        	
				try {
					JSONObject fpkj = JSONObject.parseObject(data)
							.getJSONObject("Data")
							.getJSONObject("FP_KJ");
					MapWritable map = getIndexFieldMap(fpkj);
					map.put(new Text("id"), new Text(rowKey));
	    			context.write(new Text(UUID.randomUUID().toString().replace("-", "")),map);
				} catch (Exception e1) {
					e1.printStackTrace();
				}
	        }
	    }  
		
	    private MapWritable getIndexFieldMap(JSONObject fpkj){
	    	MapWritable map = new MapWritable();
			if(fpkj.containsKey("KPRQ")&&fpkj.getString("KPRQ")!=null){
	            String KPRQ = fpkj.getString("KPRQ");
	            map.put(new Text("KPRQ"), new Text(KPRQ));
			}
	        
			if(fpkj.containsKey("FPQQLSH")&&fpkj.getString("FPQQLSH")!=null){
	            String FPQQLSH = fpkj.getString("FPQQLSH");
	            map.put(new Text("FPQQLSH"), new Text(FPQQLSH));
			}
			
			if(fpkj.containsKey("FP_DM")&&fpkj.getString("FP_DM")!=null){
	            String FP_DM = fpkj.getString("FP_DM");
	            map.put(new Text("FP_DM"), new Text(FP_DM));
			}
			
			if(fpkj.containsKey("FP_HM")&&fpkj.getString("FP_HM")!=null){
	            String FP_HM = fpkj.getString("FP_HM");
	            map.put(new Text("FP_HM"), new Text(FP_HM));
			}				
			
			if(fpkj.containsKey("DDH")&&fpkj.getString("DDH")!=null){
	            String DDH = fpkj.getString("DDH");
	            map.put(new Text("DDH"), new Text(DDH));
			}	
			
			if(fpkj.containsKey("DDSJ")&&fpkj.getString("DDSJ")!=null){
	            String DDSJ = fpkj.getString("DDSJ");
	            map.put(new Text("DDSJ"), new Text(DDSJ));
			}	
			
			if(fpkj.containsKey("XSF_NSRSBH")&&fpkj.getString("XSF_NSRSBH")!=null){
	            String XSF_NSRSBH = fpkj.getString("XSF_NSRSBH");
	            map.put(new Text("XSF_NSRSBH"), new Text(XSF_NSRSBH));
			}
			
			if(fpkj.containsKey("XSF_MC")&&fpkj.getString("XSF_MC")!=null){
	            String XSF_MC = fpkj.getString("XSF_MC");
	            map.put(new Text("XSF_MC"), new Text(XSF_MC));
			}	
			
			if(fpkj.containsKey("XSF_DZDH")&&fpkj.getString("XSF_DZDH")!=null){
	            String XSF_DZDH = fpkj.getString("XSF_DZDH");
	            map.put(new Text("XSF_DZDH"), new Text(XSF_DZDH));
			}
			
			if(fpkj.containsKey("GMF_MC")&&fpkj.getString("GMF_MC")!=null){
	            String GMF_MC = fpkj.getString("GMF_MC");
	            map.put(new Text("GMF_MC"), new Text(GMF_MC));
			}
			
			if(fpkj.containsKey("GMF_NSRSBH")&&fpkj.getString("GMF_NSRSBH")!=null){
	            String GMF_NSRSBH = fpkj.getString("GMF_NSRSBH");
	            map.put(new Text("GMF_NSRSBH"), new Text(GMF_NSRSBH));
			}				
			
			if(fpkj.containsKey("GMF_SJ")&&fpkj.getString("GMF_SJ")!=null){
	            String GMF_SJ = fpkj.getString("GMF_SJ");
	            map.put(new Text("GMF_SJ"), new Text(GMF_SJ));
			}				
			
			if(fpkj.containsKey("GMF_WX")&&fpkj.getString("GMF_WX")!=null){
	            String GMF_WX = fpkj.getString("GMF_WX");
	            map.put(new Text("GMF_WX"), new Text(GMF_WX));
			}	
			
			if(fpkj.containsKey("YFP_DM")&&fpkj.getString("YFP_DM")!=null){
	            String YFP_DM = fpkj.getString("YFP_DM");
	            map.put(new Text("YFP_DM"), new Text(YFP_DM));
			}
			
			if(fpkj.containsKey("YFP_HM")&&fpkj.getString("YFP_HM")!=null){
	            String YFP_HM = fpkj.getString("YFP_HM");
	            map.put(new Text("YFP_HM"), new Text(YFP_HM));
			}
			
			if(fpkj.containsKey("HJJE")&&fpkj.getString("HJJE")!=null){
	            String HJJE = fpkj.getString("HJJE");
	            map.put(new Text("HJJE"), new Text(HJJE));
			}					
			
			if(fpkj.containsKey("HJSE")&&fpkj.getString("HJSE")!=null){
	            String HJSE = fpkj.getString("HJSE");
	            map.put(new Text("HJSE"), new Text(HJSE));
			}	
			
			if(fpkj.containsKey("SWJG_DM")&&fpkj.getString("SWJG_DM")!=null){
	            String SWJG_DM = fpkj.getString("SWJG_DM");
	            map.put(new Text("SWJG_DM"), new Text(SWJG_DM));
			}
			
			if(fpkj.containsKey("FPZL_DM")&&fpkj.getString("FPZL_DM")!=null){
	            String FPZL_DM = fpkj.getString("FPZL_DM");
	            map.put(new Text("FPZL_DM"), new Text(FPZL_DM));
			}
			
			if(fpkj.containsKey("HY_DM")&&fpkj.getString("HY_DM")!=null){
	            String HY_DM = fpkj.getString("HY_DM");
	            map.put(new Text("HY_DM"), new Text(HY_DM));
			}
			
			if(fpkj.containsKey("GMF_EMAIL")&&fpkj.getString("GMF_EMAIL")!=null){
	            String GMF_EMAIL = fpkj.getString("GMF_EMAIL");
	            map.put(new Text("GMF_EMAIL"), new Text(GMF_EMAIL));
			}			
			
			if(fpkj.containsKey("JSHJ")&&fpkj.getString("JSHJ")!=null){
	            String JSHJ = fpkj.getString("JSHJ");
	            map.put(new Text("JSHJ"), new Text(JSHJ));
			}					
			
			if(fpkj.containsKey("JYM")&&fpkj.getString("JYM")!=null){
	            String JYM = fpkj.getString("JYM");
	            map.put(new Text("JYM"), new Text(JYM));
			}				

			if(fpkj.containsKey("GMF_QQ")&&fpkj.getString("GMF_QQ")!=null){
	            String GMF_QQ = fpkj.getString("GMF_QQ");
	            map.put(new Text("GMF_QQ"), new Text(GMF_QQ));
			}
			
			if(fpkj.containsKey("GMF_WEIBO")&&fpkj.getString("GMF_WEIBO")!=null){
	            String GMF_WEIBO = fpkj.getString("GMF_WEIBO");
	            map.put(new Text("GMF_WEIBO"), new Text(GMF_WEIBO));
			}
			
			if(fpkj.containsKey("FPLB")&&fpkj.getString("FPLB")!=null){
	            String FPLB = fpkj.getString("FPLB");
	            map.put(new Text("FPLB"), new Text(FPLB));
			}
			
			if(fpkj.containsKey("DSPTBM")&&fpkj.getString("DSPTBM")!=null){
	            String DSPTBM = fpkj.getString("DSPTBM");
	            map.put(new Text("DSPTBM"), new Text(DSPTBM));
			}
			
			return map;
	    }
	}
	
	
	public static class HBaseIndexerReducer extends Reducer<Text, MapWritable, NullWritable, NullWritable> {

	    private CloudSolrServer solr; // 只创建一个SolrServer实例  
	    private int commitSize;  
	    private final List<SolrInputDocument> inputDocs = new ArrayList<SolrInputDocument>(); 
	    
	    public static enum Counters {ROWS}; // 用于计数器
	    public static Logger log = Logger.getLogger(HBaseIndexerReducer.class);
	    
	    public static Calendar calendar = Calendar.getInstance();
		
		
		@Override
		protected void setup(Reducer<Text, MapWritable, NullWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
	        Configuration conf = context.getConfiguration();  
	        solr = new CloudSolrServer(conf.get("solr.server"));  
	        solr.setDefaultCollection(conf.get("solr.collection"));
	        commitSize = conf.getInt("solr.commit.size", 100000); // 一次性添加的文档数，写在配置文件中  
			
		}
		

		@Override
		protected void reduce(Text key, Iterable<MapWritable> values,
				Reducer<Text, MapWritable, NullWritable, NullWritable>.Context context) throws IOException, InterruptedException {
	        
			for(MapWritable map:values){
				SolrInputDocument solrDoc = new SolrInputDocument();
				for(Writable k:map.keySet()){
					//将kprq转为long类型
					if(k.toString().equals("KPRQ")){
						solrDoc.addField(k.toString(), Long.valueOf(map.get(k).toString()));
					}else{
						solrDoc.addField(k.toString(), map.get(k).toString());
					}
					
				}
				inputDocs.add(solrDoc); 
		        if (inputDocs.size() >= commitSize) {  
		            try {  
		                solr.add(inputDocs); // 索引文档  
//		                solr.commit();
		    
		                //设置线程休眠
//		                Thread.sleep(1000);
		                
		                context.getCounter(Counters.ROWS).increment(commitSize);
		                log.info("创建索引：---------"+inputDocs.size());
		                log.info("创建索引时间：-----------------------"+calendar.get(Calendar.MINUTE));
		            } catch (final SolrServerException e) {  
		                final IOException ioe = new IOException();  
		                ioe.initCause(e);  
		                throw ioe;  
		            }  
		            inputDocs.clear();  
		        }
			}
		}

		
		
		@Override
		protected void cleanup(Reducer<Text, MapWritable, NullWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
	        try {  
	            if (!inputDocs.isEmpty()) {  
	                solr.add(inputDocs);
	                solr.commit();
	                inputDocs.clear();  
	            }  
	        } catch (final SolrServerException e) {  
	            final IOException ioe = new IOException();  
	            ioe.initCause(e);  
	            throw ioe;  
	        }  
		}

	}
	
    public static Job createSubmittableJob(Configuration conf, String tablename, String[] args)  
            throws IOException {  
        //Hadoop集群调优
        conf.set("mapreduce.map.output.compress", "true");
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
        
        conf.set("mapreduce.map.java.opts", "-Xmx9216m");
        
        conf.set("mapreduce.reduce.java.opts", "-Xmx9216m");
        
        conf.set("mapreduce.reduce.shuffle.memory.limit.percent", "0.08");
        conf.set("mapreduce.job.reduce.slowstart.completedmaps", "1");
        
        //设置排序缓冲区
        conf.set("mapreduce.task.io.sort.mb", "1200");
        
		//设置任务超时时间
		conf.set("mapreduce.task.timeout", "0");
		
        
        //设置jvm重用
        conf.set("mapreduce.job.jvm.numtasks", "-1");  
        
        //关闭推测执行
        conf.set("mapreduce.map.speculative", "false");
        conf.set("mapreduce.reduce.speculative", "false");
        
        //设置hbase扫描超时时间
        conf.set("hbase.client.scanner.timeout.period", "3600000");
        conf.set("hbase.rpc.timeout", "3600000");
        
        Job job = Job.getInstance(conf, "SolrIndex_" + tablename);  
        job.setJarByClass(HBaseIndexerMain.class);  
          
        Scan scan = new Scan();  
        
        //设置扫描时间范围
//        scan.setTimeRange(1489075200000L,1489420800000L);
        //HBase优化
        scan.setCaching(10000);//每次scan返回的数据行数
        scan.setCacheBlocks(false); // scan的数据不放在缓存中，一次性的  
        scan.setMaxResultSize(4194304);//每次scan返回的数据大小
        
		/*//设置单列值过滤器,扫描时间范围
		FilterList filters = new FilterList(Operator.MUST_PASS_ALL);
		SingleColumnValueFilter startFilter = new SingleColumnValueFilter(Bytes.toBytes("Info")
				, Bytes.toBytes("KPRQ"), CompareOp.GREATER_OR_EQUAL, Bytes.toBytes(args[0]+"00000000"));
		startFilter.setFilterIfMissing(true);
		
		SingleColumnValueFilter endFilter = new SingleColumnValueFilter(Bytes.toBytes("Info")
				, Bytes.toBytes("KPRQ"), CompareOp.LESS, Bytes.toBytes(args[1]+"00000000"));
		endFilter.setFilterIfMissing(true);
		
		filters.addFilter(startFilter);
		filters.addFilter(endFilter);
		scan.setFilter(filters);*/
        
        /* 需要建索引的数据 */  
//        scan.addColumn(Bytes.toBytes("Info"), Bytes.toBytes("Data"));  
          
        TableMapReduceUtil.initTableMapperJob(tablename, scan,  
        		HBaseIndexerMapper.class, Text.class, MapWritable.class, job); // 需要输出，键、值类型 

        job.setNumReduceTasks(3); // 设置reduce任务  
        
        job.setReducerClass(HBaseIndexerReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        
        FileOutputFormat.setOutputPath(job, new Path("hdfs://DXHY-YFEB-02/Indexer"));
//        FileOutputFormat.setOutputPath(job, new Path("hdfs://nameservice/Indexer"));
        
        return job;  
    }  
	
	public static void main(String[] args) {
		
			Configuration conf = HBaseConfiguration.create();  
	        conf.setBoolean("mapred.map.tasks.speculative.execution", false); 
	        //zookeeper集群
//			conf.set("hbase.zookeeper.quorum", "HT-FPDSJ-JS01:2181,HT-FPDSJ-JS02:2181,HT-FPDSJ-JS03:2181");
	        conf.set("hbase.zookeeper.quorum", "DXHY-YFEB-01:2181,DXHY-YFEB-02:2181,DXHY-YFEB-03:2181");
	        //solr缓存数量
			conf.setInt("solr.commit.size",100000);
			//solr集群的zookeeper集群
//			conf.set("solr.server", "HT-FPDSJ-JS01:2181,HT-FPDSJ-JS02:2181,HT-FPDSJ-JS03:2181/solr");
			conf.set("solr.server", "DXHY-YFEB-01:2181,DXHY-YFEB-02:2181,DXHY-YFEB-03:2181/solr");
			//solr集群的collection
			conf.set("solr.collection", "test_invoice");
//			conf.set("solr.collection", args[2]);
	        String tablename = "test:Invoice";
			try {
				Job job = createSubmittableJob(conf, tablename,args);
				System.exit(job.waitForCompletion(true) ? 0 : 1);
				Counter counter = job.getCounters().findCounter(HBaseIndexerReducer.Counters.ROWS);
				log.info("Put " + counter.getValue() + " records to Solr!"); // 打印日志  
			} catch (IOException e) {
				e.printStackTrace();
			} catch (ClassNotFoundException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
	        
	}
}
