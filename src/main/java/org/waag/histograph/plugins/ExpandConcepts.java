package org.waag.histograph.plugins;

import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.traversal.Uniqueness;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;

import javax.ws.rs.core.Context;

@javax.ws.rs.Path( "/" )
public class ExpandConcepts {

  private GraphDatabaseService graphDb;
  private final ObjectMapper objectMapper;

  private boolean isPit(Node node) {
    boolean hasUnderscore = false;

    for (Label label: node.getLabels()) {
      final String n = label.name();

      if(n.equals("_")) {
        hasUnderscore = true;
        continue;
      }

      if(n.equals("_Rel") || n.equals("="))
        return false;
    }
    return hasUnderscore;
  }

  public ExpandConcepts(@Context GraphDatabaseService graphDb) {
    this.graphDb = graphDb;
    this.objectMapper = new ObjectMapper();
  }

  @GET
  @javax.ws.rs.Path("/ping")
  public Response ping(@Context HttpServletRequest request, final InputStream requestBody) {
    StreamingOutput stream = new StreamingOutput() {
      @Override
      public void write(OutputStream os) throws IOException, WebApplicationException {
        os.write("pong".getBytes(Charset.forName("UTF-8")));
      }
    };
    return Response.ok().entity(stream).type(MediaType.TEXT_PLAIN).build();
  }

  @POST
  @javax.ws.rs.Path("/expand")
  public Response expand(@Context HttpServletRequest request, final InputStream requestBody) {
    StreamingOutput stream = new StreamingOutput() {

      @Override
      public void write(OutputStream os) throws IOException, WebApplicationException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(requestBody));
        JsonFactory f = new JsonFactory();
        JsonParser jp = f.createJsonParser(reader);

        ExpandParameters parameters = objectMapper.readValue(jp, ExpandParameters.class);

        JsonGenerator jg = objectMapper.getJsonFactory().createJsonGenerator(os, JsonEncoding.UTF8);

        ArrayList<String> visited = new ArrayList<String>();

        TraversalDescription conceptTraversalDescription = graphDb.traversalDescription()
            .breadthFirst()
            .relationships(DynamicRelationshipType.withName(parameters.equivalence), Direction.BOTH)
            .uniqueness(Uniqueness.NODE_RECENT);

        TraversalDescription hairsTraversalDescription = graphDb.traversalDescription()
            .depthFirst();
        for (String relation : parameters.hairs) {
          hairsTraversalDescription = hairsTraversalDescription.relationships(DynamicRelationshipType.withName(relation), Direction.OUTGOING);
        }
        hairsTraversalDescription = hairsTraversalDescription.evaluator(Evaluators.fromDepth(2))
            .evaluator(Evaluators.toDepth(2));

        jg.writeStartArray();
        try (Transaction tx = graphDb.beginTx()) {
          for (String id : parameters.ids) {
            if (!visited.contains(id)) {
              Concept concept = new Concept();

              Node node = graphDb.findNode(DynamicLabel.label("_"), "id", id);

              if (node == null) {
                continue;
              }

              concept.addPit(node);
              visited.add(id);

              Traverser conceptTraverser = conceptTraversalDescription.traverse(node);

              // Get all nodes found in each path, add them to concept if they weren't added before
              for (Path path : conceptTraverser) {
                Node startNode = path.startNode();
                String startNodeId = startNode.getProperty("id").toString();

                Node endNode = path.endNode();
                String endNodeId = endNode.getProperty("id").toString();

                if (!visited.contains(endNodeId)) {

                  boolean endNodeIsPit = isPit(endNode);
                  if (endNodeIsPit) {
                    concept.addPit(endNode);
                  }

                  visited.add(endNodeId);
                }
              }

              for (Path path : conceptTraverser) {
                // Identity relation paths (always length 2) denote single identify relation
                // between two PITs in concept. Walk paths, and add them to concept!
                for (Node pathNode: path.nodes()) {
                  if (!isPit(pathNode)) {
                    Iterable<Relationship> outgoingRelations = pathNode.getRelationships(Direction.OUTGOING);
                    Iterable<Relationship> incomingRelations = pathNode.getRelationships(Direction.INCOMING);

                    Relationship outgoingRelation = outgoingRelations.iterator().next();
                    Relationship incomingRelation = incomingRelations.iterator().next();

                    String incomingStartNodeId = incomingRelation.getStartNode().getProperty("id").toString();

                    concept.addRelation(incomingStartNodeId, outgoingRelation);
                  }
                }
              }

              Traverser hairsTraverser = hairsTraversalDescription.traverse(concept.getNodes());

              // Each path is an incoming or outgoing hair: (p)-[r]-[q]
              // p belongs to the concept, q does not
              for (Path path : hairsTraverser) {
                String startNodeId = path.startNode().getProperty("id").toString();
                Node endNode = path.endNode();
                Relationship relation = path.lastRelationship();
                concept.addHair(startNodeId, relation, endNode);
              }

              concept.toJson(jg);
            }
          }
        }

        jg.writeEndArray();
        jg.flush();
        jg.close();
      }
    };
    return Response.ok().entity(stream).type(MediaType.APPLICATION_JSON).build();
  }
}
