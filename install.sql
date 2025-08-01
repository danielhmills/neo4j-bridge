-- Check for Linked Data Cartridges VAD installation, and install if missing. 
CREATE FUNCTION neo4j_bridge_dependency_install(){
 DECLARE cartridges_check, r2rml_check INTEGER;

 IF((SELECT COUNT( DISTINCT name) FROM vad_list_packages()(name VARCHAR) x WHERE LCASE(name) = 'cartridges') <> 1){
   EXEC('VAD_INSTALL(\'../vad/cartridges_dav.vad\',0)');
 };
 IF((SELECT COUNT( DISTINCT name) FROM vad_list_packages()(name VARCHAR) x WHERE LCASE(name) = 'rdb2rdf') <> 1){
  EXEC('VAD_INSTALL(\'../vad/rdb2rdf_dav.vad\',0)');
 };
   RETURN 1;
};

neo4j_bridge_dependency_install();


-- Authentication Handler
CREATE PROCEDURE neo4j_bridge_auth_values(IN neo4j_host VARCHAR, IN token_type VARCHAR := null, IN token_value VARCHAR := null, IN registry_mode VARCHAR := 'add'){
    DECLARE auth_value VARCHAR;

    IF(POSITION(LCASE(registry_mode),VECTOR('add','delete'))< 1){
        SIGNAL('invalid-entry','Please use "add" or "delete" as the mode value.');
    };
    IF(LCASE(token_type) = 'basic'){
        token_type := 'Basic';
    };
        IF(LCASE(token_type) = 'bearer'){
        token_type := 'Bearer';
    };
    IF(LCASE(registry_mode) = 'add' AND POSITION(token_type,VECTOR('basic','bearer'))){
        SIGNAL('invalid-entry','Please use "basic" or "bearer" as the mode value.');
    };

    auth_value := SPRINTF('%s %s', token_type, token_value);

    IF(registry_mode = 'add'){
        registry_set('neo4j-bridge-auth',auth_value);
        registry_set('neo4j-bridge-host', SPRINTF('%s/db/neo4j/query/v2',neo4j_host));
    }
    IF(registry_mode = 'delete'){
        registry_remove('neo4j-bridge-auth');
        registry_remove('neo4j-bridge-host');
    };
    RETURN SPRINTF('%s token complete.', registry_mode);
};

-- Optional Camelcase Procedure
CREATE PROCEDURE to_camelcase(IN _text VARCHAR, IN delimiter VARCHAR := '_'){
    DECLARE i INTEGER;
    DECLARE final_value ANY ARRAY;
    _text := REPLACE(_text,delimiter,',');
    _text := SPLIT_AND_DECODE(_text,2,'\0\0,');
    final_value := Vector(_text[0]);
    i := 1;
    WHILE(i < LENGTH(_text)){
     _text[i] := INITCAP(_text[i]);
     final_value[0] := CONCAT(final_value[0],_text[i]);
     i := i +1;
    };

    RETURN final_value[0];
};

-- Acutal Bridge
CREATE PROCEDURE neo4j_bridge(IN query VARCHAR, IN host_loc VARCHAR := null, IN auth_token VARCHAR := null){
    DECLARE query_body, error_code, error_msg, error_text, host_value, headers  VARCHAR;
    DECLARE results, cols, entity_cols, entity_path, tree, _dt, _dt_clean, props ANY ARRAY;
    DECLARE result_choice, cols_count, rows_count, i,k INT;
    query_body := SPRINTF('{"statement":"%s"}', query) ;
    IF(host_loc IS NULL){
      host_loc := (select cast(registry_get('neo4j-bridge-host') as varchar));
    };
    IF(auth_token IS NULL){
      auth_token := ( SELECT(CAST(registry_get('neo4j-bridge-auth') AS VARCHAR )));
    };
    headers := SPRINTF('Authorization: %s\nContent-Type: application/json\n', auth_token);
    results := HTTP_CLIENT(url => cast(host_loc as varchar) , http_method => 'POST', 
     http_headers => cast(headers as varchar), 
     body => query_body);
    results := JSON_PARSE(results);
    IF(JPATH_EVAL('#/errors[0]/code', results) is not null){
      error_code := JPATH_EVAL('#/errors[0]/code', results);
      error_msg := JPATH_EVAL('#/errors[0]/message', results);
      error_text := SPRINTF('%s: %s', error_code, error_msg);
      SIGNAL('neo4j-error',error_text);
    };

    tree := DB.DBA.JSONLD_TREE_TO_XML(results, 1);
    result_choice := XPATH_EVAL('name(//results/data/values[1]/*[1])',tree,1);

    IF( result_choice <> 'elementId')
    {
        cols := JPATH_EVAL('#/data/fields',results);
        EXEC_RESULT_NAMES(cols);
        results := JPATH_EVAL('#/data/values',results);
        FOREACH (ANY ARRAY x in results) DO
        {
            EXEC_RESULT(x);
        };
    }
    ELSE
    {
        tree := DB.DBA.JSONLD_TREE_TO_XML(results, 1);
        rows_count := length( XPATH_EVAL('//results/data/values',tree,0) );
        cols_count := length(XPATH_EVAL('//results/data/values[1]/properties/*',tree,0));
        entity_cols := VECTOR('elementId', 'labels');
        cols := VECTOR_CONCAT(VECTOR(), entity_cols);
        entity_cols := vector();
        i := 1;
        WHILE(i <= cols_count){
            entity_path := SPRINTF('name(//results/data/values[1]/properties/*[%s])',i);
            entity_path := XPATH_EVAL(entity_path,tree,1);
            cols := VECTOR_CONCAT(cols, VECTOR(CAST(entity_path as VARCHAR)));
            i := i + 1;
        };     
        EXEC_RESULT_NAMES(cols);

        i := 1;
        k := 1;

        WHILE(i < rows_count){
            _dt_clean := vector();
             props := XPATH_EVAL(SPRINTF('//results/data/values[%s]/properties/*/text()',i),tree,0);
            _dt := VECTOR(XPATH_EVAL( SPRINTF('//results/data/values[%s]/*[1]/text()',i),tree,1 ));
            _dt := VECTOR_CONCAT(_dt, VECTOR(XPATH_EVAL( SPRINTF('//results/data/values[%s]/*[2]/text()',i),tree,1 )));
            FOREACH(ANY x in _dt) DO{
                _dt_clean := VECTOR_CONCAT(_dt_clean,vector(CAST(x as VARCHAR)));
            };
            _dt := _dt_clean;
            EXEC_RESULT(VECTOR_CONCAT(_dt,props));
            i := i + 1;
        };
        
    }
};

