# Histograph Neo4j plugin

[Unmanaged Extension](http://neo4j.com/docs/stable/server-unmanaged-extensions.html) for Neo4j v2.2.2. Uses Neo4j's [Traversal Framework Java API](http://neo4j.com/docs/stable/tutorial-traversal-java-api.html) to compute Histograph concepts, and their outgoing relations. These outgoing relations will make all concepts nice and hairy.

![](cavia.jpg)

## Installation

First, add the following line to `neo4j-server.properties`:

    org.neo4j.server.thirdparty_jaxrs_classes=org.waag.histograph.plugins=/histograph

Afterwards, clone this repository:

    git clone https://github.com/histograph/neo4j-plugin.git

Go to directory:

    cd neo4j-plugin

And build the plugin using Maven:

    mvn package

Then, copy JAR file to Neo4j's plugin directory!

(Or just run `install.sh` (currently only works under MacOS with Neo4j installed by Homebrew).)

Alternatively, you can [directy dowload a compiled JAR](dist/histograph-plugin-0.5.0-SNAPSHOT.jar) from GitHub.

## Ping

To test if plugin is properly installed and loaded by Neo4j, run:

    curl http://localhost:7474/histograph/ping

The plugin should respond with:

    pong

## Usage

    curl -s -X POST http://localhost:7474/histograph/expand \
      -H "Content-Type: application/json" \
      -d '{"equivalence":"hg:sameHgConcept", "hairs":["hg:liesIn"], "ids": ["urn:hg:geonames:2759794", "urn:hg:geonames:2753637", "urn:hg:geonames:2753639", "urn:hg:geonames:2753638", "urn:hg:geonames:2753640", "urn:hg:tgn:7264696", "urn:hg:tgn:term:1001511217", "urn:hg:geonames:2753636", "urn:hg:tgn:7264697", "urn:hg:tgn:7264700", "urn:hg:tgn:term:1001493884"]}' \
    | python -mjson.tool | pygmentize -l js

The plugin exposes a single endpoint - `/histograph/expand` - and expects a POST request with a JSON body of the following form:

```json
{
  "equivalence": "hg:sameHgConcept",
  "hairs": [
    "hg:liesIn"
  ],
  "ids": [
    "urn:hg:geonames:2759794",
    "urn:hg:geonames:2753637",
    "..."
  ]
}
```

## Output

Example output of a single street in lovely Bussum. Response is an array of Histograph concepts, each concept contains an array of PITs:

```json
[
  [
    {
      "hairs": [],
      "pit": {
        "data": {
          "gme_id": 381,
          "gme_naam": "Bussum",
          "wpsnaamnen": "BUSSUM",
          "wvk_ids": [
            278353046,
            278353106,
            278354021,
            278354047
          ]
        },
        "geometry": {
          "coordinates": [
            [
              [
                5.158458167008441,
                52.28061009371572
              ],
              [
                5.15888270877095,
                52.27956954753817
              ]
            ],
            [
              [
                5.15888270877095,
                52.27956954753817
              ],
              [
                5.159036946991636,
                52.2792532154488
              ]
            ],
            [
              [
                5.158210740054656,
                52.281287156905684
              ],
              [
                5.158145997725702,
                52.28147231411347
              ],
              [
                5.158130279481965,
                52.28167900202832
              ],
              [
                5.158144146936424,
                52.28183182070672
              ],
              [
                5.158172668630267,
                52.28198466775494
              ],
              [
                5.158267660671528,
                52.2821293477672
              ]
            ],
            [
              [
                5.158267660671528,
                52.2821293477672
              ],
              [
                5.158342184175749,
                52.28227628896101
              ],
              [
                5.158443925510478,
                52.28238823949631
              ],
              [
                5.158671010293007,
                52.28255945475333
              ],
              [
                5.158765053654402,
                52.28271820733365
              ]
            ]
          ],
          "type": "MultiLineString"
        },
        "id": "urn:hgid:nwb/bussum-albrechtlaan",
        "name": "Albrechtlaan",
        "type": "hg:Street"
      },
      "relations": []
    },
    {
      "hairs": [
        {
          "id": "urn:hgid:bag/1331",
          "name": "Bussum",
          "type": "hg:Place"
        }
      ],
      "pit": {
        "data": {
          "woonplaatscode": 1331,
          "woonplaatsnaam": "Bussum"
        },
        "id": "urn:hgid:bag/381300000100009",
        "name": "Albrechtlaan",
        "type": "hg:Street"
      },
      "relations": [
        {
          "to": "urn:hgid:bag/1331",
          "type": "hg:liesIn"
        },
        {
          "to": "urn:hgid:nwb/bussum-albrechtlaan",
          "type": "hg:sameHgConcept"
        }
      ]
    }
  ]
]
```

Copyright (C) 2015 [Waag Society](http://waag.org).
