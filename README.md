# Naive TExt Classification Dataset Tagger

https://medium.com/@mageswaran1989/big-data-play-ground-for-engineers-flask-annotation-tool-for-text-classification-99da32a5248

## Postgresql

- install ubuntu packages
`sudo apt-get install postgresql postgresql-contrib`

- check the version
`sudo -u postgres psql -c "SELECT version();"`

- test the installation

```shell script
sudo su - postgres
    psql #to launch the terminal
    \q #to quit
```

- To run psql directly
`sudo -i -u postgres psql`

```shell script
sudo su - postgres
    psql #to launch the terminal
    # drop user sparkstreaming;
    CREATE USER tagger WITH PASSWORD 'tagger'; 
    \du #list users
    CREATE DATABASE taggerdb;
    grant all privileges on database taggerdb to tagger;
    \list # to see the DB created
    \q

# test the new user and DB
sudo -i -u tagger  psql -d taggerdb
    CREATE TABLE authors (code char(5) NOT NULL, name varchar(40) NOT NULL, city varchar(40) NOT NULL, joined_on date NOT NULL, PRIMARY KEY (code));
    INSERT INTO authors VALUES(1,'Ravi Saive','Mumbai','2012-08-15');
    \dt #list tables
    \conninfo #get connection info
```

## How to run?
- Upload the data to the DB
```
python dataset_base.py
```
- Run the tagger
```
python app.py
```
URL: http://0.0.0.0:8766/

- Download the data from the DB
```
python dataset_base.py --mode=download
```