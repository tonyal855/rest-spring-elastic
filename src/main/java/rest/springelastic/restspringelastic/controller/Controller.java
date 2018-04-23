package rest.springelastic.restspringelastic.controller;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import javax.servlet.http.HttpServletRequest;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.WrapperQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.filter.Filter;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.filters.Filters;
import org.elasticsearch.search.aggregations.bucket.filters.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.metrics.avg.AvgAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import restspring.springelastic.restspringelastic.model.Model;

@RestController
public class Controller {

	
//	  @Autowired
//	  private DiscoverServices service;
	  private @Autowired HttpServletRequest request;

	
	@Autowired
	private Client client;
	
	@RequestMapping("view/{id}")
	public Map<String, Object> view(@PathVariable final String id){
		GetResponse getResponse = client.prepareGet("twitter_timeline", "twitter_timeline", id).get();
        System.out.println(getResponse.getSource());
        
        return getResponse.getSource();

	}
	
	@RequestMapping("viewall")
	public SearchHits viewall() {
		
		
		QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("user_id.keyword", "838011586497302531"));
//		QueryBuilder queryBuilder = QueryBuilders.matchQuery("sentiment", "-1");
//		QueryBuilder queryBuilder = new BoolQueryBuilder().must(QueryBuilders.termQuery("sentiment", "-1"));
//		BoolQueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("sentiment.keyword", "0"));
		SearchResponse response = client.prepareSearch("twitter_timeline")
				.setTypes("twitter_timeline")
				.setFrom(0)
				.setSize(30)
				.setQuery(queryBuilder)
				.get();
				
		for (SearchHit responses :response.getHits()) {
//			System.out.println(responses);
//			System.out.println(responses.getId());
//			System.out.println(responses.getSourceAsString());
			System.out.println(responses.getSource().get("name"));
//			System.out.println(responses.getSource().get("tweet_text"));
			System.out.println(responses.getSource().get("sentiment"));
			System.out.println(responses.getSource().get("user_id"));

		}
		System.out.println(response.getHits());
		
				return  response.getHits();
		
	}
	
	
	@RequestMapping("userid/{user_id}")
	public  Map<String, Object> userid(@PathVariable final String user_id) {
		QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("user_id.keyword", user_id));

		SearchResponse response = client.prepareSearch("twitter_timeline")
				.setTypes("twitter_timeline")
				.setFrom(0)
				.setSize(30)
				.setQuery(queryBuilder)
				.get();
				
		for (SearchHit responses :response.getHits()) {

			System.out.println(responses.getSource().get("name"));
			System.out.println(responses.getSource().get("sentiment"));
			System.out.println(responses.getSource().get("tweet_text"));
//			Map<String, Object> asd = responses.getSource();
			
			return  responses.getSource();


		}
		return null;
		
	}
	
	@RequestMapping("getall/{screen_name}")
	public List<String> getall(@PathVariable final String screen_name) throws Exception   {
		
	//querybuilder			
//	QueryBuilder queryBuilder = QueryBuilders.boolQuery().must(QueryBuilders.matchQuery("mention", screen_name ));
//	QueryBuilder queryBuilder = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("mention", screen_name));
//	QueryBuilder queryBuilder2 = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("retweet", screen_name));	
//	QueryBuilder queryBuilder3 = QueryBuilders.boolQuery().should(QueryBuilders.matchQuery("screen_name", screen_name))
	//querybuilder json
	String queryJson = "{\"bool\":{\"should\":[{\"term\":{\"screen_name.keyword\":"+screen_name+"}}],\"boost\":1.0}}";
//	String queryJson = "{\"bool\":{\"should\":[{\"term\":{\"screen_name.keyword\":"+screen_name+"}},{\"term\":{\"retweet.keyword\":"+screen_name+"}},{\"term\":{\"mention.keyword\":"+screen_name+"}}],\"boost\":1}}";	
//	String queryJson = "{\"bool\":{\"must\":[{\"term\":{\"screen_name.keyword\":\"501Awani\"}},{\"range\":{\"timestamp\":{\"gte\":\"1512666000000\",\"lt\":\"1513097999000\"}}}]}}";
//	String queryJson = "{\"range\":{\"created_date.keyword\":{\"gte\":\"Fri Dec 08 00:00:00 WIB 2017\",\"lte\":\"Sat Dec 09 23:59:59 WIB 2017\",\"format\":\"E MMM dd HH:mm:ss zz yy||E MMM dd HH:mm:ss zz y\"}}}";
	//aggregation	
//	AggregationBuilder agg = AggregationBuilders.filter("agg", QueryBuilders.termQuery("screen_name.keyword", screen_name));
	AggregationBuilder agg = AggregationBuilders.filters("agg",
															new FiltersAggregator.KeyedFilter("netral",QueryBuilders.termQuery("sentiment.keyword", "0")),
															new FiltersAggregator.KeyedFilter("positif",QueryBuilders.termQuery("sentiment.keyword", "1")),
															new FiltersAggregator.KeyedFilter("negatif",QueryBuilders.termQuery("sentiment.keyword","-1")));
											
	JSONObject js = new JSONObject(queryJson);
	QueryBuilder qb = QueryBuilders.wrapperQuery(js.toString());
	List<String> count = new ArrayList();
	int slices = 5;
	List<String> aggResult = new ArrayList();
	 LinkedHashMap<String, Object> result = new LinkedHashMap<String, Object>();
	
	
	IntStream.range(0, slices).parallel().forEach(i -> {
		//prepare search
		SliceBuilder sliceBuilder = new SliceBuilder(i, slices);
		SearchResponse response = client.prepareSearch("twitter_timeline_v1").setTypes("twitter_timeline")
//				.setSource(searchSourceBuilder)
				.setScroll(new TimeValue(60000))
				.slice(sliceBuilder)
				.setFrom(0)
				.setSize(100)
				.setQuery(qb) 
				.addAggregation(agg)
//				.setSource(a)
				.get();
		
		Filters aggs = response.getAggregations().get("agg");
//		aggs.getDocCount();
//		System.out.println(response.getHits().getTotalHits());
//		System.out.println(aggs.getDocCount());
		for(Filters.Bucket entry : aggs.getBuckets()) {
			String key = entry.getKeyAsString();
			int docCount = (int) entry.getDocCount();
			result.put(key, docCount);
		}
		
	do {
	for (SearchHit responses : response.getHits()) {
		
		String tes1 = (String) responses.getSourceAsString();
		count.add(tes1);
	
	}
	response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();		
	}
while(response.getHits().getHits().length !=0);
	
	});
	
	System.out.println(result);		

	return count;
	
	}
	
	

	

	

@RequestMapping("date/{screen_name}")
	public LinkedHashMap<String, Object> date(@PathVariable String screen_name) throws Exception {
		String sdate = "20171209";
		String edate = "20171212";
		String asd =  null;
	
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss");
		Object start = sdf.parse(sdate.toString() + " 00:00:00").getTime();
		Object end = sdf.parse(edate.toString() + " 23:59:59").getTime();
		
//		long start = sdf.parse(sdate.toString() + " 00:00:00").getTime();
//		long end = sdf.parse(edate.toString() + " 23:59:59").getTime();

		System.out.println(start);
		System.out.println(end);
//		return null;
	
//		String queryJson = "{\"bool\":{\"filter\":[{\"term\":{\"mention.keyword\":"+screen_name+"}},{\"range\":{\"timestamp\":{\"gte\":"+start+",\"lt\":"+end+"}}}]}}";
//		String queryJson = "{\"bool\":{\"must\":[{\"term\":{\"screen_name.keyword\":"+screen_name+"}},{\"term\":{\"mention.keyword\":"+screen_name+"}},{\"term\":{\"retweet.keyword\":"+screen_name+"}},{\"range\":{\"timestamp\":{\"gte\":"+start+",\"lte\":"+end+"}}}]}}";
		
//		String queryJson = "{\"bool\":{\"filter\":{\"term\":{\"mention.keyword\":"+screen_name+"}}}}";
		String queryJson = "{\"bool\":{\"filter\":[{\"term\":{\"mention.keyword\":"+screen_name+"}},{\"range\":{\"timestamp\":{\"gte\":"+start+",\"lte\":"+end+"}}}],\"boost\":\"1\"}}";
		
		
		JSONObject js = new JSONObject(queryJson);
		QueryBuilder qb = QueryBuilders.wrapperQuery(js.toString());		
//		System.out.println(js);
//		System.out.println(qb);		
		List<String> count = new ArrayList();
		 LinkedHashMap<String, Object> result = new LinkedHashMap<String, Object>();
		int slices = 5;		
		SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.searchSource();
		IntStream.range(0, slices).parallel().forEach(i -> {
			SliceBuilder sliceBuilder = new SliceBuilder(i, slices);
			SearchResponse response = client.prepareSearch("twitter_timeline_v1").setTypes("twitter_timeline")
					.setSource(searchSourceBuilder)
					.setScroll(new TimeValue(60000))
					.slice(sliceBuilder)
					.setFrom(0)
					.setSize(10000)
					.setQuery(qb)
					.setExplain(false)
					.get();
//		System.out.println(response.getTook());

//			int numSliceResults = (int) response.getHits().totalHits();
//			numSliceResults += response.getHits().getHits().length;
//		List<String> r = Arrays.stream(response.getHits().getHits()).map(SearchHit::getSourceAsString).collect(Collectors.toList());	
////	            System.out.println("slice:" + i + "," + numSliceResults);		
////	            System.out.println("total hits:"+ response.getHits().getTotalHits());
//	            count.addAll(r);
////		System.out.println(r);
			do {
			for (SearchHit responses : response.getHits()) {
//				String tes1 = (String) responses.getSourceAsString();
				List<String> tes1 = (List<String>) responses.getSource().get("mention");
				count.addAll(tes1);			
			}			
			 response = client.prepareSearchScroll(response.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();		
			}
		while(response.getHits().getHits().length !=0);
			
			
		});
		
		
		
		
		Set<String> uniqueSet = new HashSet<String>(count);
		Map<String, Integer> map = new HashMap<String, Integer>();


		for (String temp : uniqueSet) {	
			map.put(temp, Collections.frequency(count, temp));
		}
				
		List<Map.Entry<String, Integer>> list =
                new LinkedList<Map.Entry<String, Integer>>(map.entrySet());
		
		 Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
	            public int compare(Map.Entry<String, Integer> o1,
	                               Map.Entry<String, Integer> o2) {
	                return (o1.getValue()).compareTo(o2.getValue());
	            }
	        });
		

		if(list.size() < 6) {
			 Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
		        for (Map.Entry<String, Integer> entry : list) {
		            sortedMap.put(entry.getKey(), entry.getValue());
		            result.put(entry.getKey(), entry.getValue());
		            }
		}
		else {
		 Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
	        for (Map.Entry<String, Integer> entry : list.subList(list.size()-6	, list.size()-1)) {
	            sortedMap.put(entry.getKey(), entry.getValue());
	            result.put(entry.getKey(), entry.getValue());   
	        }
		}

	  return result;
	  
  }


}



