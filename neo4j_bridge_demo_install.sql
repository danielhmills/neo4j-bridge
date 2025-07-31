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

-- Generate Physical Table for Relationship Attributes
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
