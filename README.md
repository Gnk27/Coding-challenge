# Coding Challenge Resolution

## About the solution

In the `dags` folder can be found a file containing the initialization for a DAG in charge of running the `uniprot_reader.py` file, this Python code is in charge of parsing the XML file and uploading the data to a graph database.
An empty neo4j database called `neo4j` is necesary for the code to run, implemented on `bolt://localhost:7687`.

In the `dat_model` folder a image can be found with the produced data model for this project.

> Presented by: Giancarlo Marquez