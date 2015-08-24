package org.waag.histograph.plugins;

import org.codehaus.jackson.JsonGenerator;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class Pit {
  private Node pit;
  private Set<Relationship> relations = new HashSet<Relationship>();
  private Set<Node> hairs = new HashSet<Node>();

  public static final Set<String> PROPERTY_KEYS_EXCLUDE = new HashSet<String>(Arrays.asList(new String[]{"created", "accessTime", "counter"}));
  public static final Set<String> PROPERTY_KEYS_SIMPLE = new HashSet<String>(Arrays.asList(new String[]{"id", "uri", "name", "type"}));
  public static final Set<String> PROPERTY_KEYS_RAW = new HashSet<String>(Arrays.asList(new String[]{"geometry", "data"}));

  public Pit(Node pit) {
    this.pit = pit;
  }

  public void addRelation(Relationship relation) {
    relations.add(relation);
  }

  public void addHair(Relationship relation, Node node) {
    relations.add(relation);
    hairs.add(node);
  }

  public Node getNode() {
    return pit;
  }

  public void toJson(JsonGenerator jg) throws IOException {
    jg.writeStartObject();
    jg.writeFieldName("pit");
    nodeToJson(jg, pit);

    jg.writeFieldName("relations");
    jg.writeStartArray();
    for (Relationship relation : relations) {
      relationToJson(jg, relation);
    }
    jg.writeEndArray();

    jg.writeFieldName("hairs");
    jg.writeStartArray();
    for (Node hair: hairs) {
      nodeToJson(jg, hair, true);
    }
    jg.writeEndArray();

    jg.writeEndObject();
  }

  public void nodeToJson(JsonGenerator jg, Node node, boolean simple) throws IOException {
    jg.writeStartObject();
    for (String key: node.getPropertyKeys()) {
      if (!PROPERTY_KEYS_EXCLUDE.contains(key)) {
        if (!simple || PROPERTY_KEYS_SIMPLE.contains(key)) {
          Object value = node.getProperty(key);
          if (PROPERTY_KEYS_RAW.contains(key)) {
            jg.writeFieldName(key);
            jg.writeRawValue(value.toString());
          } else {
            jg.writeObjectField(key, value);
          }
        }
      }
    }
    jg.writeEndObject();
  }

  public void nodeToJson(JsonGenerator jg, Node node) throws IOException {
    nodeToJson(jg, node, false);
  }

  public void relationToJson(JsonGenerator jg, Relationship relation) throws IOException {
    jg.writeStartObject();

    String endNodeId = relation.getEndNode().getProperty("id").toString();
    String type = relation.getType().name();
    type = type.replace('_', ':');

    jg.writeStringField("to", endNodeId);
    jg.writeStringField("type", type);

    jg.writeEndObject();
  }
}
