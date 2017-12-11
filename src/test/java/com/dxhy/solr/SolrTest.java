/*
package com.dxhy.solr;

import com.alibaba.fastjson.JSONObject;
import com.dxhy.solr.dao.BaseSolrDao;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.cloud.ZkStateReader;
import org.junit.Test;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;

public class SolrTest {
	
	public static CloudSolrServer server;
	public int i = 0;
	
	static{
		try {
//			server = new CloudSolrServer("HT-FPDSJ-JS01:2181,HT-FPDSJ-JS02:2181,HT-FPDSJ-JS03:2181,HT-FPDSJ-JS04:2181/solr",false);
			server = new CloudSolrServer("DXHY-YFEB-01,DXHY-YFEB-02,DXHY-YFEB-03/solr",false);
		    server.setZkClientTimeout(7000);
		    server.setZkConnectTimeout(3000);
//			server.connect();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}

	
	@Test
	public void test_deleteAll() throws Exception{
		
		server.setDefaultCollection("test_invoice");
		server.deleteByQuery("*:*");
		server.commit();
		
	}
	
	@Test
	public void test_query() throws Exception{
		
//		SolrQuery query = new SolrQuery();
//		query.add("collection", "Invoice_20170200000000");
//		query.setQuery("(GMF_SJ : 18636924682)");
//		query.setStart(0);
//		String countStr = Integer.toString(Short.MAX_VALUE, 10);
//		query.setRows(Integer.valueOf(countStr));
//		query.setSort(InvoiceConstants.KPRQ,ORDER.desc);
//		QueryResponse response = server.query(query);
//		SolrDocumentList list = response.getResults();
//		for(SolrDocument doc:list){
//			System.out.println(doc.getFieldValue("id"));
//		}
		SolrQuery query = new SolrQuery();
		query.add("collection", "test_invoice2");
		query.setQuery("id : 66719_f0f3c23a58d3522c92b8fce7e79f5a01");
		
		QueryResponse response = server.query(query);
		if(response.getResults().getNumFound()!=0){
			SolrDocumentList docs = response.getResults();
			for(SolrDocument doc : docs){
				System.out.println(doc.getFirstValue("DDH"));
			}
		}
		
		
		
		
		
	}
	
	@Test
	public void test_getCollections(){

		ZkStateReader reader = server.getZkStateReader();
		Set<String> set = reader.getClusterState().getCollections();
		System.out.println(reader.getClusterState().getCollections().size());
		for(String s : set){
			System.out.println(s);
		}
		
	}
	
	@Test
	public void test_clusterState(){

//		ZkStateReader reader = server.getZkStateReader();
//		Set<String> set = reader.getClusterState().getCollections();
//		System.out.println(reader.getClusterState().getCollections().size());
//		for(String s : set){
//			System.out.println(s);
//		}
    	Object[] objects = server.getZkStateReader().getClusterState().getCollections().toArray();
    	System.out.println(objects[objects.length-2].toString());
	}
	
	@Test
	public void test_sortList(){
		
		BaseSolrDao dao = new BaseSolrDao();
		System.out.println(dao.getCollections("20170100000000","20170200000000"));
		

	}
	
	public String getCollections(List<String> collections,String startDate,String endDate){
		
		List<String> copyCollections = new ArrayList<String>();
		copyCollections.addAll(collections);
		int start = 0;
		int end = 0;
		
		if(!copyCollections.contains(startDate)&&!copyCollections.contains(endDate)){
			
			copyCollections.add(startDate);
			copyCollections.add(endDate);
			Collections.sort(copyCollections);
			start = Collections.binarySearch(copyCollections, startDate)-1;
			end = Collections.binarySearch(copyCollections, endDate)-1;
			
		}else if(!copyCollections.contains(startDate)&&copyCollections.contains(endDate)){
			
			copyCollections.add(startDate);
			Collections.sort(copyCollections);
			start = Collections.binarySearch(copyCollections, startDate)-1;
			end = Collections.binarySearch(copyCollections, endDate);
			
		}else if(copyCollections.contains(startDate)&&!copyCollections.contains(endDate)){
			
			copyCollections.add(endDate);
			Collections.sort(copyCollections);
			start = Collections.binarySearch(copyCollections, startDate);
			end = Collections.binarySearch(copyCollections, endDate);
			
		}else if(copyCollections.contains(startDate)&&copyCollections.contains(endDate)){
			
			Collections.sort(copyCollections);
			start = Collections.binarySearch(copyCollections, startDate);
			end = Collections.binarySearch(copyCollections, endDate);
			
		}
		
		start = collections.indexOf(copyCollections.get(start));
		end = collections.indexOf(copyCollections.get(end));
		
		StringBuilder sb = new StringBuilder();
		for(int i = start;i<=end;i++){
			
			if(i==end){
				sb.append(collections.get(i));
				break;
			}
			
			sb.append(collections.get(i));
			sb.append(",");
		}
		
		return sb.toString();
	}
	
	
	public String getCollection(List<String> collections,String kprq){
		
		List<String> copyCollections = new ArrayList<String>();
		copyCollections.addAll(collections);
		int index = 0;
		
		if(copyCollections.contains("Invoice_"+kprq)){
			
			Collections.sort(copyCollections);
			index = Collections.binarySearch(copyCollections, "Invoice_"+kprq);
			
		}else if(!copyCollections.contains("Invoice_"+kprq)){
			
			copyCollections.add("Invoice_"+kprq);
			Collections.sort(copyCollections);
			index = Collections.binarySearch(copyCollections, "Invoice_"+kprq)-1;
			
		}
		
		index = collections.indexOf(copyCollections.get(index));
		
		return collections.get(index);
	}
	
	@Test
	public void test_Query() throws Exception{
		
		server.setDefaultCollection("Invoice_20170400000000");
//		server.setDefaultCollection("Invoice_20170600000000");
		SolrQuery query = new SolrQuery();
		query.setQuery("XSF_NSRSBH : 110192562134916 AND DDH : 9847390367");
		QueryResponse response = server.query(query);
		System.out.println(response.getResults());
	}
	
	@Test
	public void test_List(){
		String[] collections = new String[2];
		collections[0] = "a";
		collections[1] = "b";
    	int length = collections.length;
    	QueryResponse response = null;
    	for(int i=length-1;i>=0;i--){
    		
    		System.out.println(collections[i]);
    		
    	}
		
	}
	
	@Test
	public void test_show(){
		
		String count = Integer.toString(Integer.MAX_VALUE, 10);
		System.out.println(count);
		
	}
	

	@Test
	public void test02(){
		
		JSONObject object = new JSONObject();
		object.put("FP_KJMX", "");
		System.out.println(object.get("FP_KJMX").equals(""));
		
		
	}
	
	@Test
	public void test_add() throws SolrServerException, IOException{
		
//		server.setDefaultCollection("test_Insert2");
//		List<SolrInputDocument> docs = new ArrayList<SolrInputDocument>();
//		for(int i=101;i<=200;i++){
//			SolrInputDocument doc = new SolrInputDocument();
//			doc.addField("id", String.valueOf(i));
//			doc.addField("myname", "zc");
//			doc.addField("age", Long.valueOf(i));
//			docs.add(doc);
//		}
//		server.add(docs);
//		server.commit();
//		
//		
		SolrQuery query = new SolrQuery();
		query.add("collection", "test_Insert2");
		query.setQuery("myname:zc AND age:[1 TO 10]");
		query.addFilterQuery(new String[]{});
		QueryResponse response = server.query(query);
		System.out.println(response.getResults().getNumFound());
		
		
		
		
		
	}
	
}
*/
