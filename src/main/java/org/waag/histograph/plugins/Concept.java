package org.waag.histograph.plugins;

import org.codehaus.jackson.JsonGenerator;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.io.IOException;
import java.util.*;

public class Concept implements Comparable<Concept> {

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
    for (String id: pits.keySet()) {
      Pit pit = pits.get(id);

      for (Relationship relation: pit.getRelations()) {
        String toId = relation.getEndNode().getProperty("id").toString();
        Pit toPit = pits.get(toId);
        if (toPit != null) {
          toPit.incrementIncomingCount();
        }
      }
    }

    List<Pit> pitValues = new ArrayList(pits.values());
    Collections.sort(pitValues);
    jg.writeStartArray();
    for (Pit pit : pitValues) {
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

  public int size() {
    return pits.size();
  }

  @Override
  public int compareTo(Concept c) {
    return c.size() - size();
  }
}