## NDjson batch importer for SQL

Importer for reddit bz2 archives

---
### Docker

Starting container
```
docker-compose build db
docker-compose up db
```

Stopping container
```
docker-compose down
```

### Scripts

* single.py - Single threaded row by row import using autocommit 1
* batch.py - Batch commits using pandas (fastest)
* setup.sql - Initial script run when starting container. Sets up tables and constraints.