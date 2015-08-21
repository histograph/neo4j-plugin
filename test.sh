#!/usr/bin/env bash

curl -s -X POST http://localhost:7474/histograph/expand \
  -H "Content-Type: application/json" \
  -d '{"ids": ["urn:hg:geonames:2759794", "urn:hg:geonames:2753637", "urn:hg:geonames:2753639", "urn:hg:geonames:2753638", "urn:hg:geonames:2753640", "urn:hg:tgn:7264696", "urn:hg:tgn:term:1001511217", "urn:hg:geonames:2753636", "urn:hg:tgn:7264697", "urn:hg:tgn:7264700", "urn:hg:tgn:term:1001493884"],"equivalence":"hg:sameHgConcept", "hairs":["hg:liesIn"]}' \
| python -mjson.tool | pygmentize -l js
