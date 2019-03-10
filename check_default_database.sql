SELECT sp.name
    , sp.default_database_name
FROM sys.server_principals sp
WHERE sp.name = SUSER_SNAME();