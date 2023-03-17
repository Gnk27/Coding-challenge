import xml.etree.ElementTree as ET
from neo4j_connector import App


def read_xml_file():
    tree = ET.parse('data\Q9Y261.xml')

    root = tree.getroot()
    entryName = ''
    for child in root:
        if 'entry' in child.tag:
            create_entry(child)
            print(child)
        for sub_child in child:
            tag = str(sub_child.tag).replace(
                "{http://uniprot.org/uniprot}", "")
            if 'accession' == tag:
                create_accession(sub_child.text)
                continue
            if 'name' == tag:
                entryName = sub_child.text
                update_entry_name(entryName)
                continue
            if 'protein' == tag:
                protein_details = []
                for recommended_name in sub_child.iter('{http://uniprot.org/uniprot}recommendedName'):
                    recommended_name_full_name = recommended_name.find(
                        '{http://uniprot.org/uniprot}fullName').text
                    recommended_name_short_names = [short_name.text for short_name in recommended_name.findall(
                        '{http://uniprot.org/uniprot}shortName')]
                protein_details.append(
                    {'recommended_name': recommended_name_full_name, 'short_names': recommended_name_short_names})
                create_protein(protein_details, entryName)
                protein_details = []
                for alternative_name in sub_child.iter('{http://uniprot.org/uniprot}alternativeName'):
                    alternative_name_full_name = alternative_name.find(
                        '{http://uniprot.org/uniprot}fullName').text
                    alternative_name_short_names = [short_name.text for short_name in alternative_name.findall(
                        '{http://uniprot.org/uniprot}shortName')]
                    protein_details.append(
                        {'alternative_name': alternative_name_full_name, 'short_names': alternative_name_short_names})
                for detail in protein_details:
                    create_alt_names(detail)
                continue
            if 'gene' == tag:
                gene_name_primary = sub_child.find(
                    '{http://uniprot.org/uniprot}name[@type="primary"]').text
                gene_synonyms = []
                for name in sub_child.findall('{http://uniprot.org/uniprot}name[@type="synonym"]'):
                    gene_synonyms.append(name.text)
                create_gene(gene_name_primary, gene_synonyms)
                continue
            if 'organism' == tag:
                scientific_name = sub_child.find(
                    '{http://uniprot.org/uniprot}name[@type="scientific"]').text
                common_name = sub_child.find(
                    '{http://uniprot.org/uniprot}name[@type="common"]').text
                tax_id = sub_child.find(
                    '{http://uniprot.org/uniprot}dbReference[@type="NCBI Taxonomy"]').attrib['id']
                create_organism(scientific_name, common_name, tax_id)
                for taxon in sub_child.find('{http://uniprot.org/uniprot}lineage'):
                    create_lineage(scientific_name,taxon.text)
                continue
            if 'reference' == tag:
                citation = sub_child.find(
                    '{http://uniprot.org/uniprot}citation')
                create_reference(sub_child.get('key'), citation)
                for author in citation.find('{http://uniprot.org/uniprot}authorList'):
                    create_author(sub_child.get('key'), author.get('name'))
                continue
            if 'feature' == tag:
                feature = {}
                feature['id'] = sub_child.get('id')
                feature['type'] = sub_child.get('type')
                feature['description'] = sub_child.get('description')
                feature['evidence'] = sub_child.get('evidence')
                location = sub_child.find(
                    '{http://uniprot.org/uniprot}location')
                feature['location_begin'] = location.find('{http://uniprot.org/uniprot}begin').get(
                    'position') if location.find('{http://uniprot.org/uniprot}begin') is not None else ''
                feature['location_end'] = location.find('{http://uniprot.org/uniprot}end').get(
                    'position') if location.find('{http://uniprot.org/uniprot}end') is not None else ''
                feature['location_position'] = location.find('{http://uniprot.org/uniprot}position').get(
                    'position') if location.find('{http://uniprot.org/uniprot}position') is not None else ''
                create_feature(feature)
                continue
            if 'evidence' == tag:
                evidence = {}
                evidence['type'] = sub_child.get('type')
                evidence['key'] = sub_child.get('key')
                source = sub_child.find('{http://uniprot.org/uniprot}source')
                evidence['source_type'] = source.find(
                    '{http://uniprot.org/uniprot}dbReference').get('type') if source is not None else ''
                evidence['source_id'] = source.find(
                    '{http://uniprot.org/uniprot}dbReference').get('id')  if source is not None else ''
                create_evidence(evidence)
                continue
            #print(sub_child.tag, sub_child.attrib, sub_child.text)
    print("Process finished")


def create_entry(node):
    app = init_neoj_connection()
    result = app.create_entry(node, 'Q9Y261')
    print(result)
    app.close()


def create_accession(name):
    app = init_neoj_connection()
    result = app.create_accession(name, 'Q9Y261')
    print(result)
    app.close()


def update_entry_name(name):
    app = init_neoj_connection()
    result = app.update_entry_name(name, 'Q9Y261')
    print(result)
    app.close()


def create_protein(properties, entryName):
    app = init_neoj_connection()
    result = app.create_protein(properties, 'Q9Y261', entryName)
    print(result)
    app.close()


def create_alt_names(properties):
    app = init_neoj_connection()
    result = app.create_alt_names(properties, 'Q9Y261')
    print(result)
    app.close()


def create_gene(gene_name_primary, gene_synonyms):
    app = init_neoj_connection()
    result = app.create_gene(gene_name_primary, gene_synonyms, 'Q9Y261')
    print(result)
    app.close()


def create_organism(scientific_name, common_name, tax_id):
    app = init_neoj_connection()
    result = app.create_organism(
        scientific_name, common_name, tax_id, 'Q9Y261')
    print(result)
    app.close()


def create_lineage(scientific_name, lineage):
    app = init_neoj_connection()
    result = app.create_lineage(scientific_name, lineage)
    print(result)
    app.close()


def create_reference(key, citation):
    app = init_neoj_connection()
    result = app.create_reference(key, citation, 'Q9Y261')
    print(result)
    app.close()


def create_author(key, author):
    app = init_neoj_connection()
    result = app.create_author(key, author)
    print(result)
    app.close()


def create_feature(feature):
    app = init_neoj_connection()
    result = app.create_feature(feature, 'Q9Y261')
    print(result)
    app.close()

def create_evidence(evidence):
    app = init_neoj_connection()
    result = app.create_evidence(evidence, 'Q9Y261')
    print(result)
    app.close()


def init_neoj_connection():
    uri = "bolt://localhost:7687"
    user = "neo4j"
    password = "Passw0rd"
    app = App(uri, user, password)
    return app


if __name__ == "__main__":
    read_xml_file()
