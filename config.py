from Redis import RedisDB
from PostgreSQL import PostgreSQLDB
from MongoDB import MongoDB

databases = {
    'Redis': RedisDB,
    'PostgreSQL': PostgreSQLDB,
    'MongoDB': MongoDB
}