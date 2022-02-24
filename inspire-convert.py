
# import required module
import base64
import hashlib
import json
import os
import urllib.parse
from pathlib import Path

import requests
import yaml
from lxml import etree
from rdflib import DC, DCTERMS, SKOS, BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCAT, FOAF, RDF
from tqdm import tqdm

# assign directory

def get_config(file):
    my_path = Path(__file__).resolve()  # resolve to get rid of any symlinks
    config_path = my_path.parent / file
    with config_path.open() as config_file:
        config = yaml.load(config_file, Loader=yaml.FullLoader)
    return config

# iterate over files in
# that directory

def add_value_in_dict(sample_dict, key, value):
    ''' Append multiple values to a key in 
        the given dictionary '''
    if key not in sample_dict:
        sample_dict[key] = list()
    sample_dict[key].append(value)
    return sample_dict

def converter():

    config = get_config('config-convert.yaml')
    directory = config['input']['folder']
    filelist = os.listdir(directory)

    g = Graph()
    VCARD = Namespace(config['rdf']['vcard']['uri'])
    namespace = config['rdf']['vcard']['namespace']
    g.bind(namespace, VCARD)

    file_bar = tqdm(filelist, desc=config['bar']['description'], colour=config['bar']['colour'], leave=config['bar']['leave'])
    for filename in file_bar:
        f = os.path.join(directory, filename)
        # checking if it is a file
        if os.path.isfile(f) and os.path.splitext(f)[-1].lower() == config['input']['extension']:
            # print(f)

            namefile = (Path(f).stem)
            dataset_location =  namefile.split("-")[1]    
            dataset_identifier = namefile.replace(" ","_")
            #print(dataset_identifier)

            file_bar.set_description(namefile.split("-")[0])
            # etree.register_namespace("gmd", "http://www.isotc211.org/2005/gmd")
            # etree.register_namespace("gco", "http://www.isotc211.org/2005/gco")
            tree = etree.parse(f)
            root = tree.getroot()
            
            # https://stackoverflow.com/questions/4210730/how-do-i-use-xml-namespaces-with-find-findall-in-lxml#answer-20689138
            # as of 1053-Italy.xml the namespace is insisde the gco:CharacterString, so it cannot be retrieved from the root.nsmap

            nsmap = config['xml']['nsmap']
            # nsmap = {}
            # for ns in root.xpath('//namespace::*'):
            #    if ns[0]: # Removes the None namespace, neither needed nor supported.
            #     nsmap[ns[0]] = ns[1]
            
            identifier = root.xpath(config['xml']['xpath']['identifier'], namespaces=nsmap)[0].text
            dataset_dct_identifier = ""
            if (len(identifier)):
                dataset_dct_identifier = identifier
            
            #print(dataset_dct_identifier)
            
            title = root.xpath(config['xml']['xpath']['title'], namespaces=nsmap)[0].text
            dataset_title = ""
            if (len(title)):
                dataset_title = title
            
            #print(dataset_title)

            dataset_keywords = []
            keywords = root.xpath(config['xml']['xpath']['keywords'], namespaces=nsmap)
            for keyword in keywords:
                dataset_keywords.append(keyword.text)
            
            #print(dataset_keywords)
            
            datathemes = root.xpath(config['xml']['xpath']['datathemes']['anchor'], namespaces=nsmap)
            dataset_themes = {}
            for datatheme in datathemes:
                text = datatheme.text
                link = datatheme.xpath(config['xml']['xpath']['datathemes']['link'], namespaces=nsmap)
                if(len(link) and len(link[0]) and (text is not None) and len(text)): #len(link) to see if href is present, len(link(0) and len(text) to not add empty values
                    link = link[0]
                    dataset_themes = add_value_in_dict(dataset_themes, link, text)
                else:  #106-Spain.xml, Anchor does not have href
                    if((text is not None) and len(text)):
                        dataset_keywords.append(text)
            
            dataset_keywords = list(set(dataset_keywords))
            #print(dataset_themes)

            issued_date = root.xpath(config['xml']['xpath']['date'], namespaces=nsmap)
            issued_datetime = root.xpath(config['xml']['xpath']['datetime'], namespaces=nsmap)
            dataset_issued = ""
            if (len(issued_date) and len(issued_date[0].text)):
                dataset_issued = issued_date[0].text
            else:
                if (len(issued_datetime) and len(issued_datetime[0].text) ):
                    dataset_issued = issued_datetime[0].text 
            
            #print(dataset_issued)

            abstract = root.xpath(config['xml']['xpath']['abstract'], namespaces=nsmap)
            dataset_description = ""
            if (len(abstract) and len(abstract[0].text)):
                dataset_description = abstract[0].text
            
            #print(dataset_description)

            responsible_parties =  root.xpath(config['xml']['xpath']['contactpoints']['party'], namespaces=nsmap)
            dataset_contact_points = []
            for party in responsible_parties:
                organization_names = party.xpath(config['xml']['xpath']['contactpoints']['name'], namespaces=nsmap)
                if (len(organization_names) and (organization_names[0].text is not None)):
                    dataset_contact_points.append(organization_names[0].text)
            
            responsible_parties =  root.xpath(config['xml']['xpath']['contacts']['party'], namespaces=nsmap)
            for party in responsible_parties:
                organization_names = party.xpath(config['xml']['xpath']['contacts']['name'], namespaces=nsmap)
                if (len(organization_names) and (organization_names[0].text is not None)):
                    dataset_contact_points.append(organization_names[0].text)
            
            dataset_contact_points = list(set(dataset_contact_points))
            # print(dataset_contact_points)

            

            responsible_parties =  root.xpath(config['xml']['xpath']['distributors']['party'], namespaces=nsmap)
            dataset_publishers = []
            for party in responsible_parties:
                organization_names = party.xpath(config['xml']['xpath']['distributors']['name'], namespaces=nsmap)
                if (len(organization_names) and (organization_names[0].text is not None)):
                    dataset_publishers.append(organization_names[0].text)

            dataset_publishers = list(set(dataset_publishers))
            #print(dataset_publishers)

            
            # n.Person  # as attribute
            dataset = URIRef(config['rdf']['uri']['dataset'] + dataset_identifier)
            g.add((dataset, RDF.type, DCAT.Dataset))
            g.add((dataset, DCTERMS.identifier, Literal(dataset_dct_identifier)))
            g.add((dataset, DCTERMS.title, Literal(dataset_title)))
            hash_object = hashlib.sha1(dataset_location.encode())
            locationURI = URIRef(config['rdf']['uri']['location'] + hash_object.hexdigest())
            g.add((dataset, DCTERMS.spatial, locationURI))
            g.add((locationURI,  RDF.type, DCTERMS.Location))
            g.add((locationURI,  DCTERMS.title, Literal(dataset_location)))
            for keyword in dataset_keywords:
                 g.add((dataset, DCAT.keyword, Literal(keyword))) 
            for key in dataset_themes:
                themeURI = URIRef(urllib.parse.quote(key))
                g.add((dataset, DCAT.theme, themeURI))
                g.add((themeURI, RDF.type, SKOS.Concept))
                list_labels = dataset_themes[key]
                for i, label in enumerate(list_labels):
                    if(i == 0):
                        g.add((themeURI, SKOS.prefLabel, Literal(label)))
                    else:
                        g.add((themeURI, SKOS.altLabel, Literal(label)))
            g.add((dataset, DCTERMS.issued, Literal(dataset_issued)))
            g.add((dataset, DCTERMS.description, Literal(dataset_description)))
            for contact_point in dataset_contact_points:
                hash_object = hashlib.sha1(contact_point.encode())
                contact_pointURI = URIRef(config['rdf']['uri']['contactpoint'] + hash_object.hexdigest())
                g.add((dataset, DCAT.contactPoint, contact_pointURI))
                g.add((contact_pointURI, RDF.type, VCARD.Kind))
                g.add((contact_pointURI, VCARD.title, Literal(contact_point)))
            for i, publisher in enumerate(dataset_publishers):
                if(i == 0): # lmit to make it DCAT-AP 2.0 compliant
                    hash_object = hashlib.sha1(publisher.encode())
                    publisherURI = URIRef(config['rdf']['uri']['publisher'] + hash_object.hexdigest())
                    g.add((dataset, DCTERMS.publisher, publisherURI))
                    g.add((publisherURI, RDF.type, FOAF.Agent))
                    g.add((publisherURI, FOAF.name, Literal(publisher)))
    
    print(config['message']['export'])
    outputfile = config['output']['folder'] + "/" + config['output']['file']
    g.serialize(destination=outputfile, format=config['output']['format'],encoding='utf-8')
    g.close()

    print(config['message']['validation'])
    url = config['validation']['url']
    txt = Path(outputfile).read_text(encoding='utf-8')
    myobj = { "contentSyntax": config['validation']['inputsyntax'], 
              "contentToValidate": txt, 
              "validationType" : config['validation']['version'], 
              "reportSyntax": config['validation']['outputsyntax'] }

    response = requests.post(url, json=myobj)
    # print("Status Code", response.status_code)

    result_text_json = response.text
    my_json= json.loads(result_text_json)
    if(my_json.get(config['validation']['jsonconform'])):
        print(config['message']['conform'])
    else:
        print(config['message']['notconform'])
        print(result_text_json)

converter()
