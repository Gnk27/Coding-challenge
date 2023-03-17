# Example code to load some data to Neo4j
# This code is based on the example code from the Neo4j Python Driver
# https://neo4j.com/docs/api/python-driver/current/

from neo4j import GraphDatabase
import logging
from neo4j.exceptions import ServiceUnavailable


class App:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def create_entry(self, entry, id):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(
                self._create_entry, entry, id)
            return result

    @staticmethod
    def _create_entry(tx, entry, id):
        query = (
            "CREATE (p1:Entry { name:'" + id + "' , " + ", ".join(
                [f"{key}: '{value}'" for key, value in entry.attrib.items()])+" }) "
        )
        result = tx.run(query)
        try:
            return result
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_accession(self, name, id):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(
                self._create_accession, name, id)
            return result

    @staticmethod
    def _create_accession(tx, name, id):
        query = (
            "CREATE (a1:Accession { name:'" + name + "' }) "
            "WITH a1 "
            "MATCH (e1:Entry) "
            "WHERE e1.name = '" + id + "' "
            "CREATE (e1)-[:HAS_ACCESSION]->(a1) "
        )
        result = tx.run(query)
        try:
            return result
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def update_entry_name(self, name, id):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(
                self._update_entry_name, name, id)
            return result

    @staticmethod
    def _update_entry_name(tx, name, id):
        query = (
            "MATCH (e1:Entry { name:'" + id + "' }) "
            "SET e1.name = '" + name + "' "
        )
        result = tx.run(query)
        try:
            return result
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_protein(self, properties, id, entryName):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(
                self._create_protein, properties, id, entryName)
            return result

    @staticmethod
    def _create_protein(tx, properties, id, entryName):
        query = (
            "CREATE (a1:Protein { id:'" + id + "' }) "
            "CREATE (p1:FullName { name: '"+properties[0]['recommended_name'] +
            "', short_names:'" +
            ", ".join(properties[0]['short_names'])+"' }) "
            "CREATE (a1)-[:HAS_FULL_NAME]->(p1) "
            "WITH a1 "
            "MATCH (e1:Entry) "
            "WHERE e1.name = '" + entryName + "' "
            "CREATE (e1)-[:HAS_PROTEIN]->(a1) "
        )
        result = tx.run(query)
        try:
            return result
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_alt_names(self, properties, id):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(
                self._create_alt_name, properties, id)
            return result

    @staticmethod
    def _create_alt_name(tx, properties, id):
        query = (
            "CREATE (a1:AltName { name:'" + properties['alternative_name'] +
            "', short_names:'"+", ".join(properties['short_names']) + "' }) "
            "WITH a1 "
            "MATCH (e1:Protein) "
            "WHERE e1.id = '" + id + "' "
            "CREATE (e1)-[:HAS_ALT_NAME]->(a1) "
        )
        result = tx.run(query)
        try:
            return result
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_gene(self, gene_name_primary, gene_synonyms, id):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(
                self._create_gene, gene_name_primary, gene_synonyms, id)
            return result

    @staticmethod
    def _create_gene(tx, gene_name_primary, gene_synonyms, id):
        query = (
            "CREATE (a1:Gene { status:'primary', name:'" +
            gene_name_primary+"', synonyms:'"+", ".join(gene_synonyms)+"' }) "
            "WITH a1 "
            "MATCH (e1:Protein) "
            "WHERE e1.id = '" + id + "' "
            "CREATE (e1)-[:FROM_GENE]->(a1) "
        )
        result = tx.run(query)
        try:
            return result
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_organism(self, scientific_name, common_name, tax_id, id):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(
                self._create_organism, scientific_name, common_name, tax_id, id)
            return result

    @staticmethod
    def _create_organism(tx, scientific_name, common_name, tax_id, id):
        query = (
            "CREATE (a1:Organism { name:'" +
            scientific_name+"', common_name:'"+common_name+"', taxonomy_id :'"+tax_id+"' }) "
            "WITH a1 "
            "MATCH (e1:Protein) "
            "WHERE e1.id = '" + id + "' "
            "CREATE (e1)-[:IN_ORGANISM]->(a1) "
        )
        result = tx.run(query)
        try:
            return result
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_lineage(self, scientific_name, lineage):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(
                self._create_lineage, scientific_name, lineage)
            return result

    @staticmethod
    def _create_lineage(tx, scientific_name, lineage):
        query = (
            "CREATE (a1:Lineage { name:'" + lineage+"' }) "
            "WITH a1 "
            "MATCH (e1:Organism) "
            "WHERE e1.name = '" + scientific_name + "' "
            "CREATE (e1)-[:HAS_LINEAGE]->(a1) "
        )
        result = tx.run(query)
        try:
            return result
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_reference(self, key, citation, id):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(
                self._create_reference, key, citation, id)
            return result

    @staticmethod
    def _create_reference(tx, key, citation, id):
        query = (
            "CREATE (a1:Reference { id:'" + key +
            "', type:'" + (citation.get('type') if citation.get('type') else '-') + "', date:'"+(citation.get('date') if citation.get('date') else '-') +
            "', name :'"+(citation.get('name') if citation.get('name') else '-') +
            "', volume :'"+(citation.get('volume')if citation.get('volume') else '-') +
            "', first :'"+(citation.get('first') if citation.get('first') else '-') +
            "', last :'"+(citation.get('last') if citation.get('last') else '-') +
            "', title :'"+(citation.find('{http://uniprot.org/uniprot}title').text if citation.find('{http://uniprot.org/uniprot}title').text else '-') +
            "' }) "
            "WITH a1 "
            "MATCH (e1:Protein) "
            "WHERE e1.id = '" + id + "' "
            "CREATE (e1)-[:HAS_REFERENCE]->(a1) "
        )
        result = tx.run(query)
        try:
            return result
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_author(self, key, author):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(
                self._create_author, key, author)
            return result

    @staticmethod
    def _create_author(tx, key, author):
        query = (
            "MERGE (a1:Author { name:'" + author+"' }) "
            "WITH a1 "
            "MATCH (e1:Reference) "
            "WHERE e1.id = '" + key + "' "
            "CREATE (e1)-[:HAS_AUTHOR]->(a1) "
        )
        result = tx.run(query)
        try:
            return result
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_feature(self, feature, id):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(
                self._create_feature, feature, id)
            return result

    @staticmethod
    def _create_feature(tx, feature, id):
        query = (
            "CREATE (a1:Feature { " + ", ".join(
                [f"{key}: '{value}'" for key, value in feature.items()])+" }) "
            "WITH a1 "
            "MATCH (e1:Protein) "
            "WHERE e1.id = '" + id + "' "
            "CREATE (e1)-[:HAS_FEATURE]->(a1) "
        )
        result = tx.run(query)
        try:
            return result
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise

    def create_evidence(self, evidence, id):
        with self.driver.session(database="neo4j") as session:
            result = session.execute_write(
                self._create_evidence, evidence, id)
            return result

    @staticmethod
    def _create_evidence(tx, evidence, id):
        query = (
            "CREATE (a1:Evidence { " + ", ".join(
                [f"{key}: '{value}'" for key, value in evidence.items()])+" }) "
            "WITH a1 "
            "MATCH (e1:Protein) "
            "WHERE e1.id = '" + id + "' "
            "CREATE (e1)-[:HAS_EVIDENCE]->(a1) "
        )
        result = tx.run(query)
        try:
            return result
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(
                query=query, exception=exception))
            raise
