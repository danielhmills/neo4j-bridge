LOG_ENABLE(3);

-- Cleanup

DROP VIEW demo.neo4j.pg2rdf_https;
DROP VIEW demo.neo4j.relationship_attrs;
DROP TABLE demo.neo4j.pg2rdf_physical;
DROP TABLE demo.neo4j.relationship_attrs_physical;
DROP PROCEDURE demo.neo4j.update_physical_table;
SPARQL DROP QUAD MAP <urn:neo4j:r2ml:mapping>;
SPARQL CLEAR GRAPH <urn:neo4j:r2ml:mapping>;
SPARQL CLEAR GRAPH <urn:inference:lpg2rdf>;
RDFS_RULE_SET('urn:inference:lpg2rdf:rule','urn:inference:lpg2rdf',1);

-- Create Procedure View
CREATE PROCEDURE VIEW demo.neo4j.pg2rdf_https AS neo4j_bridge(query)(entity1_id VARCHAR, entity1_type VARCHAR, entity1_key VARCHAR, entity1_value VARCHAR, relationship VARCHAR, entity2_id VARCHAR, entity_2_type VARCHAR, entity2_key VARCHAR, entity2_value VARCHAR);
CREATE PROCEDURE VIEW demo.neo4j.relationship_attrs AS neo4j_bridge (query)(relationship_id VARCHAR,relationship_type VARCHAR, entity1_id VARCHAR, entity2_id VARCHAR, relationship_property_key VARCHAR, relationship_property_value VARCHAR,relationship_property_value_datatype VARCHAR);

-- Generate Physical Tables
CREATE TABLE demo.neo4j.pg2rdf_physical
AS
(
    SELECT * 
    FROM demo.neo4j.pg2rdf_https
    WHERE query = 'MATCH (a)-[r]->(b) WITH a, b, r, properties(a) AS entity1_props, properties(b) AS entity2_props, labels(a) AS entity1_labels, labels(b) AS entity2_labels UNWIND entity1_labels AS entity1_label UNWIND entity2_labels AS entity2_label UNWIND keys(entity1_props) AS key1 UNWIND keys(entity2_props) AS key2 WITH elementId(a) AS entity1_id, toString(entity1_label) AS entity1_type, toString(apoc.text.camelCase(key1)) AS entity1_key, toString(entity1_props[key1]) AS entity1_value, toString(apoc.text.camelCase(type(r))) AS relationship, elementId(b) AS entity2_id, toString(entity2_label) AS entity2_type, toString(apoc.text.camelCase(key2)) AS entity2_key, toString(entity2_props[key2]) AS entity2_value RETURN entity1_id, entity1_type, entity1_key, entity1_value, relationship, entity2_id, entity2_type, entity2_key, entity2_value'
)
WITH DATA;

-- Generate Physical Tables
CREATE TABLE demo.neo4j.relationship_attrs_physical
AS
(
    SELECT * 
    FROM demo.neo4j.relationship_attrs
    WHERE query = 'MATCH (a)-[r]->(b) UNWIND keys(r) AS raw_key WITH elementId(r) AS relationship_id, type(r) AS raw_type, elementId(a) AS entity1_id, elementId(b) AS entity2_id, raw_key, r[raw_key] AS raw_value, split(toLower(raw_key), ''_'') AS key_parts, split(toLower(type(r)), ''_'') AS type_parts WITH relationship_id, entity1_id, entity2_id, raw_value, CASE WHEN raw_value IS NULL THEN [] WHEN raw_value IN [true, false] THEN [raw_value] WHEN raw_value =~ ''.*'' THEN [raw_value] ELSE raw_value END AS value_list, key_parts[0] + reduce(s = '''', part IN key_parts[1..] | s + toUpper(substring(part, 0, 1)) + substring(part, 1)) AS relationship_property_key, type_parts[0] + reduce(s = '''', part IN type_parts[1..] | s + toUpper(substring(part, 0, 1)) + substring(part, 1)) AS relationship_type UNWIND value_list AS value WITH relationship_id, relationship_type, entity1_id, entity2_id, relationship_property_key, toString(value) AS relationship_property_value, CASE WHEN value IS NULL THEN ''null'' WHEN value IN [true, false] THEN ''boolean'' WHEN toString(value) =~ ''^-?\\\\d+$'' THEN ''integer'' WHEN toString(value) =~ ''^-?\\\\d+\\\\.\\\\d+$'' THEN ''float'' ELSE ''string'' END AS relationship_property_value_datatype RETURN relationship_id, relationship_type, entity1_id, entity2_id, relationship_property_key, relationship_property_value, relationship_property_value_datatype'
)
WITH DATA;

-- Procedure To Update Physical Tables
 CREATE PROCEDURE demo.neo4j.update_physical_table(){
  DELETE FROM demo.neo4j.pg2rdf_physical;
  INSERT INTO demo.neo4j.pg2rdf_physical SELECT * FROM demo.neo4j.pg2rdf_https WHERE query = 'MATCH (a)-[r]->(b) WITH a, b, r, properties(a) AS entity1_props, properties(b) AS entity2_props, labels(a) AS entity1_labels, labels(b) AS entity2_labels UNWIND entity1_labels AS entity1_label UNWIND entity2_labels AS entity2_label UNWIND keys(entity1_props) AS key1 UNWIND keys(entity2_props) AS key2 WITH elementId(a) AS entity1_id, toString(entity1_label) AS entity1_type, toString(apoc.text.camelCase(key1)) AS entity1_key, toString(entity1_props[key1]) AS entity1_value, toString(apoc.text.camelCase(type(r))) AS relationship, elementId(b) AS entity2_id, toString(entity2_label) AS entity2_type, toString(apoc.text.camelCase(key2)) AS entity2_key, toString(entity2_props[key2]) AS entity2_value RETURN entity1_id, entity1_type, entity1_key, entity1_value, relationship, entity2_id, entity2_type, entity2_key, entity2_value';
  DELETE FROM demo.neo4j.relationship_attrs_physical;
  INSERT INTO demo.neo4j.relationship_attrs_physical SELECT * FROM demo.neo4j.relationship_attrs WHERE query = 'MATCH (a)-[r]->(b) UNWIND keys(r) AS raw_key WITH elementId(r) AS relationship_id, type(r) AS raw_type, elementId(a) AS entity1_id, elementId(b) AS entity2_id, raw_key, r[raw_key] AS raw_value, split(toLower(raw_key), ''_'') AS key_parts, split(toLower(type(r)), ''_'') AS type_parts WITH relationship_id, entity1_id, entity2_id, raw_value, CASE WHEN raw_value IS NULL THEN [] WHEN raw_value IN [true, false] THEN [raw_value] WHEN raw_value =~ ''.*'' THEN [raw_value] ELSE raw_value END AS value_list, key_parts[0] + reduce(s = '''', part IN key_parts[1..] | s + toUpper(substring(part, 0, 1)) + substring(part, 1)) AS relationship_property_key, type_parts[0] + reduce(s = '''', part IN type_parts[1..] | s + toUpper(substring(part, 0, 1)) + substring(part, 1)) AS relationship_type UNWIND value_list AS value WITH relationship_id, relationship_type, entity1_id, entity2_id, relationship_property_key, toString(value) AS relationship_property_value, CASE WHEN value IS NULL THEN ''null'' WHEN value IN [true, false] THEN ''boolean'' WHEN toString(value) =~ ''^-?\\\\d+$'' THEN ''integer'' WHEN toString(value) =~ ''^-?\\\\d+\\\\.\\\\d+$'' THEN ''float'' ELSE ''string'' END AS relationship_property_value_datatype RETURN relationship_id, relationship_type, entity1_id, entity2_id, relationship_property_key, relationship_property_value, relationship_property_value_datatype';
  RETURN 0;
};

-- Install R2RML Script

SPARQL
prefix rr: <http://www.w3.org/ns/r2rml#>
prefix neo4j: <http://demo.openlinksw.com/schemas/neo4j-demo#>
prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
prefix owl: <http://www.w3.org/2002/07/owl#>

INSERT INTO GRAPH <urn:neo4j:r2ml:mapping>
{
    <#TriplesMapNeo4jEntity1> a rr:TriplesMap; rr:logicalTable [ rr:sqlQuery """SELECT DISTINCT entity1_id, entity1_type, entity1_key, entity1_value FROM demo.neo4j.pg2rdf_physical"""]; 
    rr:subjectMap [ rr:termType rr:IRI  ; rr:template "http://demo.openlinksw.com/neo4j-demo/{entity1_id}#this"; rr:class owl:Thing; rr:graph <http://demo.openlinksw.com/neo4j/lpg2rdf#> ];
    rr:predicateObjectMap [ rr:predicateMap [ rr:constant rdf:type ] ; rr:objectMap [ rr:termType rr:IRI; rr:template "http://demo.openlinksw.com/schemas/neo4j-demo#{entity1_type}" ]; ] ;
    rr:predicateObjectMap [ rr:predicateMap [ rr:termType rr:IRI; rr:template "http://demo.openlinksw.com/schemas/neo4j-demo#{entity1_key}" ] ; rr:objectMap [ rr:column "entity1_value" ]; ] .

    <#TriplesMapNeo4jEntity2> a rr:TriplesMap; rr:logicalTable [ rr:sqlQuery """SELECT DISTINCT entity2_id, entity_2_type, entity2_key, entity2_value FROM demo.neo4j.pg2rdf_physical"""]; 
    rr:subjectMap [ rr:termType rr:IRI  ; rr:template "http://demo.openlinksw.com/neo4j-demo/{entity2_id}#this"; rr:class owl:Thing; rr:graph <http://demo.openlinksw.com/neo4j/lpg2rdf#> ];
    rr:predicateObjectMap [ rr:predicateMap [ rr:constant rdf:type ] ; rr:objectMap [ rr:termType rr:IRI; rr:template "http://demo.openlinksw.com/schemas/neo4j-demo#{entity_2_type}" ]; ] ;
    rr:predicateObjectMap [ rr:predicateMap [ rr:termType rr:IRI; rr:template "http://demo.openlinksw.com/schemas/neo4j-demo#{entity2_key}" ] ; rr:objectMap [ rr:column "entity2_value" ]; ] .

    <#TriplesMapNeo4jRelationships> a rr:TriplesMap; rr:logicalTable [ rr:sqlQuery """SELECT DISTINCT entity1_id, relationship, entity2_id FROM demo.neo4j.pg2rdf_physical"""]; 
    rr:subjectMap [ rr:termType rr:IRI  ; rr:template "http://demo.openlinksw.com/neo4j-demo/{entity1_id}#this"; rr:graph <http://demo.openlinksw.com/neo4j/lpg2rdf#> ];
    rr:predicateObjectMap [ rr:predicateMap [ rr:termType rr:IRI; rr:template "http://demo.openlinksw.com/schemas/neo4j-demo#{relationship}" ] ; rr:objectMap [ rr:termType rr:IRI  ; rr:template "http://demo.openlinksw.com/neo4j-demo/{entity2_id}#this" ]; ] .

    <#TriplesMapNeo4jRelationshipsMeta> a rr:TriplesMap; rr:logicalTable [ rr:sqlQuery """SELECT DISTINCT entity1_id, entity2_id, relationship_id, MD5(CONCAT(entity1_id,entity2_id,relationship_property_key)) as relationship_hash, relationship_property_key, relationship_property_value FROM demo.neo4j.relationship_attrs_physical"""]; 
    rr:subjectMap [ rr:termType rr:IRI  ; rr:template "http://demo.openlinksw.com/neo4j-demo/{relationship_hash}#this"; rr:class neo4j:PropertyAnnotation; rr:graph <http://demo.openlinksw.com/neo4j/lpg2rdf#> ];
    rr:predicateObjectMap [ rr:predicateMap [ rr:constant rdfs:label ] ; rr:objectMap [ rr:column "relationship_property_key" ]; ];
    rr:predicateObjectMap [ rr:predicateMap [ rr:constant neo4j:sourceNode ] ; rr:objectMap [ rr:termType rr:IRI  ; rr:template "http://demo.openlinksw.com/neo4j-demo/{entity1_id}#this" ]; ] ;
    rr:predicateObjectMap [ rr:predicateMap [ rr:constant neo4j:nodeRelationship ] ; rr:objectMap [ rr:termType rr:IRI  ; rr:template "http://demo.openlinksw.com/schemas/neo4j-demo#{relationship_property_key}#this" ]; ] ;
    rr:predicateObjectMap [ rr:predicateMap [ rr:constant neo4j:targetNode ] ; rr:objectMap [ rr:termType rr:IRI  ; rr:template "http://demo.openlinksw.com/neo4j-demo/{entity2_id}#this" ]; ] ;
    rr:predicateObjectMap [ rr:predicateMap [ rr:constant neo4j:targetValue ] ; rr:objectMap [ rr:column "relationship_property_value" ]; ] .
};

EXEC ('SPARQL ' || DB.DBA.R2RML_MAKE_QM_FROM_G ('urn:neo4j:r2ml:mapping','urn:neo4j:r2ml:map'));	

-- Setup Inference Rule

SPARQL 
PREFIX neo4j: <http://demo.openlinksw.com/schemas/neo4j-demo#>
prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
prefix owl: <http://www.w3.org/2002/07/owl#>
prefix dbr: <http://dbpedia.org/resource/>

INSERT INTO GRAPH <urn:inference:lpg2rdf>{

# Properties
# RDFS
 neo4j:name rdfs:subPropertyOf rdfs:label.
 neo4j:name rdfs:subPropertyOf schema:name.
 neo4j:title rdfs:subPropertyOf rdfs:label.
 neo4j:title rdfs:subPropertyOf schema:title.
 neo4j:tagline rdfs:subPropertyOf schema:alternativeHeadline.
 neo4j:released rdfs:subPropertyOf schema:datePublished.
 neo4j:born rdfs:subPropertyOf schema:birthDate.
 neo4j:follows rdfs:subPropertyOf schema:follows.

# OWL
 schema:actor owl:inverseOf neo4j:actedIn.
 schema:director owl:inverseOf neo4j:directed.
 schema:producer owl:inverseOf schema:produced.
 schema:author   owl:inverseOf neo4j:wrote.

# Classes
# OWL
neo4j:Person owl:equivalentClass schema:Person.
neo4j:Movie  owl:equivalentClass schema:Movie.
};

-- Insert Inference Rules from RDF View

SPARQL
INSERT INTO GRAPH <urn:inference:lpg2rdf>
{?s rdfs:subPropertyOf ?o}
WHERE{ GRAPH <http://demo.openlinksw.com/neo4j/lpg2rdf#> {?s rdfs:subPropertyOf ?o.}};

-- Add Rules into a Rule Set
RDFS_RULE_SET('urn:inference:lpg2rdf:rule','urn:inference:lpg2rdf');

-- Add Optional owl:sameAs relationship example 
SPARQL
INSERT INTO GRAPH <urn:lpg2rdf:sameAs>
{
  <http://demo.openlinksw.com/neo4j-demo/4%3A555bef39-07a8-41bf-9389-242557cc62b0%3A1#this> owl:sameAs <http://dbpedia.org/resource/Keanu_Reeves>.

};

SPARQL CLEAR GRAPH <urn:lpg2rdf:data>;
SPARQL
INSERT INTO GRAPH <urn:lpg2rdf:data>{
    ?s ?p ?o
}
WHERE{
    GRAPH <http://demo.openlinksw.com/neo4j/lpg2rdf#>{
        ?s ?p ?o
        FILTER(?p != rdfs:subPropertyOf).
    }
};

-- Example Query
SPARQL
DEFINE input:inference 'urn:inference:lpg2rdf:rule'
PREFIX neo4j: <http://demo.openlinksw.com/schemas/neo4j-demo#>
SELECT DISTINCT *

FROM <urn:lpg2rdf:data>
WHERE
{
  ?movie schema:title ?movieTitle;
   schema:actor ?actor.
  ?actor schema:name ?actorName.

 ?movieAnnotation a neo4j:PropertyAnnotation;
 neo4j:sourceNode ?actor;
 neo4j:nodeRelationship ?relationship;
 neo4j:targetNode ?movie;
 neo4j:targetValue 'Neo'.
}
