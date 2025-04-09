# Neo4j Bridge for Virtuoso
This repository contains a set of Virtuoso/PL procedures that allow Virtuoso to act as a bridge to a Neo4j graph database via HTTP calls. It includes functions for authentication management, utility string transformation, and the core query bridge itself.
![DBeaver Example](https://www.openlinksw.com/DAV/www2.openlinksw.com/data/gifs/neo4j_bridge_dbeaver.gif)

## Overview

## Procedures Included:
### 1. `neo4j_bridge_auth_values`

Registers or removes authorization credentials for the Neo4j bridge.

```sql
neo4j_bridge_auth_values (
    IN  neo4j_host   VARCHAR,
    IN  token_type   VARCHAR := NULL,    -- 'basic' or 'bearer'
    IN  token_value  VARCHAR := NULL,    -- Actual token string
    IN  registry_mode VARCHAR := 'add'   -- 'add' or 'delete'
)
```

#### Example:
```sql
neo4j_bridge_auth_values('http://localhost:7474','Bearer','my-secret-token','add');
```

> Stores credentials and endpoint for later use.

---

### 2. `to_camelcase`

Optional helper function to convert a delimited string (default delimiter: `_`) to camelCase.

```sql
to_camelcase (IN _text VARCHAR,IN delimiter  VARCHAR := '_')
```

#### Example:
```sql
SELECT to_camelcase('ACTED_IN');  -- Returns 'actedIn'
```

---

### 3. `neo4j_bridge`

Sends a Cypher query to Neo4j and returns the result in SQL tabular format.

```sql
neo4j_bridge (
    IN query       VARCHAR,     -- Cypher query string
    IN host_loc    VARCHAR := NULL,  -- Optional override for Neo4j host
    IN auth_token  VARCHAR := NULL   -- Optional override for token
)
```

If `host_loc` or `auth_token` is not provided, the function retrieves them from the Virtuoso registry as previously stored by `neo4j_bridge_auth_values`.

#### Example:
```sql
neo4j_bridge('MATCH (p:Person) RETURN p.name LIMIT 5');
```

#### Example Using a Derived Table
```sql
SELECT *
FROM neo4j_bridge(query)(name) x
WHERE query = 'MATCH (p:Person) RETURN p.name LIMIT 5'
```
---
---

## Usage

### Installation

1. Navigate to your Virtuoso installation's `database` directory, and run `git clone https://github.com/danielhmills/neo4j-bridge.git`
> Advanced Users: You can also clone the repo to a location listed in your virtuoso.ini config's DIRS_ALLOWED parameter.

2. Install the bridge using iSQL or your preferred interface to run the install.sql script.

Example
```
 isql {host} {port} {username} {password} install.sql
```

3. Add your Neo4j Instance Credentials using: `neo4j_bridge_auth_values(host, token_type, token_value, 'add')`

### Querying
4. Run a test query to your Neo4j instance using `neo4j_bridge('RETURN \'Hello World\'')`
![Hello World Test](https://www.openlinksw.com/DAV/www2.openlinksw.com/data/gifs/neo4j_bridge_hello_world.gif)

### Integration
#### Derived Tables and Procediew Views
Use a Derived Table or a Procedure View to integrate the result set within Virtuoso

**Derived Table Example**
```sql
SELECT *
FROM neo4j_bridge(query)(name VARCHAR) x
WHERE query = 'MATCH (p:Person) RETURN p.name'
```
![Derived Table Test](https://www.openlinksw.com/DAV/www2.openlinksw.com/data/gifs/neo4j_bridge_derived_table.gif)

**Procedure View Example**
```sql
CREATE PROCEDURE VIEW neo4j_bridge_test AS neo4j_bridge(query)(name VARCHAR);
SELECT * FROM neo4j_bridge_test
WHERE query = 'MATCH (p:Person) RETURN p.name'
```
#### Creating Tables and Views of Result Sets
SQL Tables and views of the result sets can be created using `CREATE TABLE` and `CREATE VIEW` with a derived table or Procedure/Query View.

**Create Table Example**
```sql
CREATE TABLE demo.neo4j.test
AS
(
 SELECT *
 FROM neo4j_bridge(query)(name VARCHAR) x
 WHERE query = 'MATCH p:Person RETURN p.name'
)
WITH DATA
```


**Create View Example**
```sql
CREATE VIEW demo.neo4j.test
AS
(
 SELECT *
 FROM neo4j_bridge(query)(name VARCHAR) x
 WHERE query = 'MATCH p:Person RETURN p.name'
)
```
---

## Error Handling

- `neo4j_bridge` will `SIGNAL` a `neo4j-error` if the response contains an error from the Neo4j server.
- `neo4j_bridge_auth_values` validates mode (`add` or `delete`) and token type (`basic` or `bearer`) and raises an error for invalid input.

---

## Dependencies

- Virtuoso Universal Server (Commercial Edition)
- Neo4j Server with the V2 Query API Enabled
- A valid Neo4j authentication token (Basic or Bearer)

---

## License

MIT License

---

## Contributing

Issues and pull requests welcome. If you have suggestions for improving the query parsing or supporting more Neo4j features (e.g., multi-statement, parameters), feel free to open a ticket.
