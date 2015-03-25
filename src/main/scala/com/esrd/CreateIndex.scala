package com.esrd

import org.elasticsearch.client.transport.TransportClient
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.common.settings.ImmutableSettings
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.action.search.SearchType
import org.elasticsearch.search.SearchHit
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.index.query.FilterBuilders
import com.fasterxml.jackson.databind.ObjectMapper

object CreateIndex extends App {
  val mapper = new ObjectMapper()

  val client = {
    val settings = ImmutableSettings.settingsBuilder()
      .put("cluster.name", "piyush-elasticsearch").put("client.transport.sniff", true).build()
    new TransportClient(settings)
      .addTransportAddress(new InetSocketTransportAddress("localhost", 9300))
  }

  val locationTypeMap = Map(0 -> "continent_id", 1 -> "country_id", 2 -> "metro_code_id", 3 -> "region_id", 4 -> "area_code_id", 5 -> "city_id", 6 -> "postal_code_id")

  def queryForLocations(locationType: Int, id: Int) = {
    var results = Array[SearchHit]()
    var scrollResp = client.prepareSearch("locations").setQuery(QueryBuilders.matchQuery(locationTypeMap(locationType), id))
      .setSearchType(SearchType.SCAN)
      .setScroll(new TimeValue(60000))
      .setSize(10).execute().actionGet();

    do {
      results = results ++ scrollResp.getHits.getHits
      scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet()
    } while (scrollResp.getHits().getHits().length != 0)
    results
  }

  var scrollResp = client.prepareSearch("human_locations")
    .setSearchType(SearchType.SCAN)
    .setScroll(new TimeValue(60000))
    .setSize(10).execute().actionGet();

  do {
    val bulkRequest = client.prepareBulk();
    for (hit <- scrollResp.getHits().getHits()) {
      val source = hit.getSource
      val id = source.get("id").asInstanceOf[Int]

      val location = source.get("location_type").asInstanceOf[Int]
      val human_name = source.get("human_name")

      
      val data = queryForLocations(location, id).toList map {
        record =>
          val fields = record.getSource 
         val c =  Map((0 to 6).toList map { id => locationTypeMap(id) -> fields.get(locationTypeMap(id).toString) }:_*)

      }
      

//      queryForLocations(location, id) foreach { hit =>
//        val source = hit.getSource
//        source.put("human_name", human_name)
//        bulkRequest.add(client.prepareIndex("geoindex", "geo")
//          .setSource(source))
//
//      }

//      println("searching for[" + id + "]" + "human_name[" + human_name + "]")
//
//      val bulkResponse = bulkRequest.execute().actionGet()
//      if (bulkResponse.hasFailures()) {
//        println("failed" + bulkResponse.buildFailureMessage())
//
//      }

    }
    scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
  } while (scrollResp.getHits().getHits().length != 0)

}

case class GeoMap(locationType: String, locationTypeId: Int, human_name: String,
                  continents_id: Array[Int], countries_ids: Array[Int], metro_code_ids: Array[Int],
                  region_ids: Array[Int], area_code_ids: Array[Int], city_ids: Array[Int])