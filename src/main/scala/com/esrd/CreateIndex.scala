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
      val human_name = source.get("human_name").asInstanceOf[String]

      val bulkRequest = client.prepareBulk();

      val totalHits = queryForLocations(location, id)
      val data1 = totalHits map (_.getSource)
      val finalData = (data1.foldLeft(Map[String, List[Object]]()) { (a, b) => combineMap(b.toMap, a) }) map { case (key, value) => key -> value.distinct }
      val data = totalHits map { hit =>
        val source = hit.getSource

        import org.json4s._
        import org.json4s.jackson.Serialization
        import org.json4s.jackson.Serialization.write
        implicit val formats = Serialization.formats(NoTypeHints)

        val continentsId = finalData.get("continent_id")

        def getFeildvalue(feildName: String) = finalData.get(feildName).get

        val index = Index(location, locationTypeMap(location), human_name, getFeildvalue("continent_id"), getFeildvalue("latitude"), getFeildvalue("timezone_id"), getFeildvalue("postal_code_id"),
          getFeildvalue("region_id"), getFeildvalue("longitude"), getFeildvalue("id"), getFeildvalue("metro_code_id"), getFeildvalue("city_id"),
          getFeildvalue("country_id"), getFeildvalue("area_code_id"))
        val data = write(index)
        println(data)
        bulkRequest.add(client.prepareIndex("geoindex2", "geo2")
          .setSource(data))

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


case class Index(location_id: Int, locationType: String, human_name: String, continent_id: List[Object], latitude: List[Object], timezone_id: List[Object], postal_code_id: List[Object],
               region_id: List[Object], longitude: List[Object], id: List[Object], metro_code_id: List[Object], city_id: List[Object],
               country_id: List[Object], area_code_id: List[Object])