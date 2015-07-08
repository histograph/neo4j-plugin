package org.waag.histograph.plugins;

import org.codehaus.jackson.JsonGenerator;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Concept {
  private Map<String, Pit> pits = new HashMap<String, Pit>();

  public void addPit(Node node) {
    String id = node.getProperty("id").toString();
    Pit pit = new Pit(node);
    pits.put(id, pit);
  }

  public void addRelation(String pitId, Relationship relation) {
    Pit pit = pits.get(pitId);
    if (pit != null) {
      pit.addRelation(relation);
    }
  }

  public void addHair(String pitId, Relationship relation, Node node) {
    Pit pit = pits.get(pitId);
    if (pit != null) {
      pit.addHair(relation, node);
    }
  }

  public void toJson(JsonGenerator jg) throws IOException {
    jg.writeStartArray();
    for (Pit pit : pits.values()) {
      pit.toJson(jg);
    }
    jg.writeEndArray();
  }

  public List<Node> getNodes() {
    List<Node> nodeList = new ArrayList<Node>();
    for (Pit pit : pits.values()) {
      nodeList.add(pit.getNode());
    }
    return nodeList;
  }
}