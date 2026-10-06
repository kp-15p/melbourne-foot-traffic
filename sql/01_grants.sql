-- Analysts get Gold and nothing else. Three grants, not one:
-- traversal on the container, then the object privilege.
GRANT USE CATALOG ON CATALOG ped_prod      TO `account users`;
GRANT USE SCHEMA  ON SCHEMA  ped_prod.gold TO `account users`;
GRANT SELECT      ON SCHEMA  ped_prod.gold TO `account users`;

-- Deliberately NOT granted: bronze, silver, landing, ops.
-- Without USE SCHEMA, a reader cannot see those schemas exist.
-- Exclusion comes from not granting, not from DENY.

-- Verify from the catalog's own metadata:
SELECT grantee, privilege_type, catalog_name
FROM   ped_prod.information_schema.catalog_privileges
WHERE  grantee = 'account users';

SELECT grantee, privilege_type, schema_name
FROM   ped_prod.information_schema.schema_privileges
WHERE  grantee = 'account users';