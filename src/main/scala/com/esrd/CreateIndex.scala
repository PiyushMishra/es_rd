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
import scala.collection.immutable.HashMap
import scala.collection.JavaConversions._

object CreateIndex extends App {
  val mapper = new ObjectMapper()

  val client = {
    val settings = ImmutableSettings.settingsBuilder()
      .put("cluster.name", "piyush-elasticsearch").put("client.transport.sniff", true).build()
    new TransportClient(settings)
      .addTransportAddress(new InetSocketTransportAddress("localhost", 9300))
  }

  val locationTypeMap = Map(0 -> "continent_id", 1 -> "country_id", 2 -> "metro_code_id", 3 -> "region_id", 4 -> "area_code_id", 5 -> "city_id", 6 -> "postal_code_id")

  def queryForHumanLocations = {
    client.prepareSearch("human_locations")
      .setSearchType(SearchType.DFS_QUERY_THEN_FETCH)
      .setQuery(QueryBuilders.matchAllQuery()).setSize(300) // Query
      .execute()
      .actionGet();
  }

  def queryForLocations(locationType: Int, id: Int) = {
    var results = Array[SearchHit]()
    var scrollResp = client.prepareSearch("locations").setQuery(QueryBuilders.matchQuery(locationTypeMap(locationType), id))
      .setSearchType(SearchType.SCAN)
      .setScroll(new TimeValue(60000))
      .setSize(300).execute().actionGet();

    do {
      results = results ++ scrollResp.getHits.getHits
      scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet()
    } while (scrollResp.getHits().getHits().length != 0)
    results
  }

  def combineMap(map1: Map[String, Object], map2: Map[String, List[Object]]) = {
    map1 map {
      case (key, value) => if (map2.contains(key)) { key -> (List(value) ++ map2(key)) } else key -> List(value)
    }
  }

  var scrollResp = client.prepareSearch("human_locations")
    .setSearchType(SearchType.SCAN)
    .setScroll(new TimeValue(60000))
    .setSize(300).execute().actionGet();

  do {
    for (hit <- scrollResp.getHits().getHits()) {
      val source = hit.getSource
      val id = source.get("id").asInstanceOf[Int]

      val location = source.get("location_type").asInstanceOf[Int]
      val human_name = source.get("human_name")

      val mmm = Map("locationType" -> (locationTypeMap(location)),
        ("location_type_id" -> location), ("human_name" -> human_name))

      val bulkRequest = client.prepareBulk();

      val totalHits = queryForLocations(location, id)
      val data1 = totalHits map (_.getSource)
      val finalData = (data1.foldLeft(Map[String, List[Object]]()) { (a, b) => combineMap(b.toMap, a) })
      val data = totalHits map { hit =>
        val source = hit.getSource
        val mmm = HashMap("locationType" -> (locationTypeMap(location)),
          ("location_type_id" -> location.asInstanceOf[Object]), ("human_name" -> human_name.asInstanceOf[Object]))

        source.putAll(finalData)
        bulkRequest.add(client.prepareIndex("geoindex2", "geo2")
          .setSource(source))

      }
      println("searching for[" + id + "]" + "human_name[" + human_name + "]totalhits[" + totalHits.size + "]")

      val bulkResponse = bulkRequest.execute().actionGet()
      if (bulkResponse.hasFailures()) {
        println("failed" + bulkResponse.buildFailureMessage())

      }

    }
    scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(600000)).execute().actionGet();
  } while (scrollResp.getHits().getHits().length != 0)
}