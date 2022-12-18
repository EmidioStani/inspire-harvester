
# import required module
import base64
import hashlib
import json
import os
import shutil
import time
from multiprocessing.pool import Pool
from pathlib import Path
from urllib.parse import quote, urlencode, urlparse, urlunparse

import requests
import validators
import yaml
from lxml import etree
from pyparsing import empty
from rdflib import DC, DCTERMS, SKOS, BNode, Graph, Literal, Namespace, URIRef
from rdflib.namespace import DCAT, FOAF, RDF
from tqdm import tqdm
from transformers import (AutoModel, AutoModelForSequenceClassification,
                          AutoTokenizer, pipeline)
from txtai.pipeline import Labels
from werkzeug import urls

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

def converter(config):

    # labels = Labels()
    # classifier = pipeline('zero-shot-classification',model='joeddav/xlm-roberta-large-xnli')
    classifier = pipeline('zero-shot-classification',model='MoritzLaurer/mDeBERTa-v3-base-mnli-xnli')
    tags = config['labels']
    directory = config['input']['folder']
    filelist = os.listdir(directory)

    file_bar = tqdm(filelist, desc=config['bar']['files']['description'], colour=config['bar']['files']['colour'], leave=config['bar']['files']['leave'])
    for index, filename in enumerate(file_bar):
        f = os.path.join(directory, filename)
        # checking if it is a file
        if os.path.isfile(f) and os.path.splitext(f)[-1].lower() == config['input']['extension']:
            # print(f)

            g = Graph()
            VCARD = Namespace(config['rdf']['vcard']['uri'])
            namespace = config['rdf']['vcard']['namespace']
            g.bind(namespace, VCARD)

            HVD = Namespace(config['rdf']['hvd']['uri'])
            namespace = config['rdf']['hvd']['namespace']
            g.bind(namespace, HVD)

            namefile = (Path(f).stem)
            dataset_location =  namefile.split("-")[1]    
            dataset_identifier = namefile.replace(" ","_")
            #print(dataset_identifier)

            file_bar.set_description(namefile)
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

            language_code = root.xpath(config['xml']['xpath']['language']['code'], namespaces=nsmap)
            language_2_code = root.xpath(config['xml']['xpath']['language2'], namespaces=nsmap)
            dataset_language = ""
            if(language_code):
                dataset_language = language_code[0].xpath(config['xml']['xpath']['language']['value'], namespaces=nsmap)[0]
            if(language_2_code):
                dataset_language = language_2_code[0].text
            iso_639_1 = ""
            if(len(dataset_language) and dataset_language != "zxx"):
                iso_639_1 = config['map']['languages'][dataset_language]
            
            title = root.xpath(config['xml']['xpath']['title'], namespaces=nsmap)
            dataset_title = ""
            if (title):
                dataset_title = title[0].text
            else:
                titles = root.xpath(config['xml']['xpath']['titlefreetext'], namespaces=nsmap)
                for title in titles:
                    if iso_639_1.upper() in title.xpath(config['xml']['xpath']['titlefreetextlanguage'], namespaces=nsmap)[0]:
                        dataset_title = title.text
            #print(dataset_title)

            dataset_keywords = []
            keywords = root.xpath(config['xml']['xpath']['keywords'], namespaces=nsmap)
            for keyword in keywords:
                if(keyword.text is not None):
                    dataset_keywords.append(keyword.text)
            
            #print(dataset_keywords)
            
            datathemes = root.xpath(config['xml']['xpath']['datathemes']['anchor'], namespaces=nsmap)
            dataset_themes = {}
            for datatheme in datathemes:
                text = datatheme.text
                link = datatheme.xpath(config['xml']['xpath']['datathemes']['link'], namespaces=nsmap)
                if(len(link) and len(link[0]) and (text is not None) and len(text)): #len(link) to see if href is present, len(link(0) and len(text) to not add empty values
                    link = link[0]
                    if (not urlparse(link)[0]):
                        link = "http://" + link
                    dataset_themes = add_value_in_dict(dataset_themes, link, text)
                else:  #106-Spain.xml, Anchor does not have href
                    if((text is not None) and len(text)):
                        dataset_keywords.append(text)
            
            dataset_keywords = list(set(dataset_keywords))
            #print(dataset_themes)

            topics = root.xpath(config['xml']['xpath']['topic'], namespaces=nsmap)
            dataset_topics = []
            for topic in topics:
                topic_text = topic.text
                if((topic_text is not None) and len(topic_text)):
                        # print("topic: " + topic_text)
                        dataset_topics.append(topic_text)

            issued_date = root.xpath(config['xml']['xpath']['date'], namespaces=nsmap)
            issued_datetime = root.xpath(config['xml']['xpath']['datetime'], namespaces=nsmap)
            dataset_issued = ""
            if (len(issued_date) and len(issued_date[0].text)):
                dataset_issued = issued_date[0].text
            else:
                if (len(issued_datetime) and len(issued_datetime[0].text) ):
                    dataset_issued = issued_datetime[0].text 
            
            #print(dataset_issued)
            abstract_list = root.xpath(config['xml']['xpath']['abstract'], namespaces=nsmap)
            dataset_description = ""
            for abstract in abstract_list:
                    if(abstract.text):
                        dataset_description = abstract.text
            else:
                abstract2_list = root.xpath(config['xml']['xpath']['abstractfreetext'], namespaces=nsmap)
                for abstract in abstract2_list:
                    if iso_639_1.upper() in abstract.xpath(config['xml']['xpath']['abstractfreetext'], namespaces=nsmap)[0]:
                        dataset_description = abstract.text
            #print(dataset_description)
            # start_time = time.time()
            # dataset_assigned_label = tags[labels(dataset_description, tags)[0][0]]
            # dataset_assigned_label = ""
            # end_time = time.time()
            # print("Time:", end_time - start_time)
            if(index < 1000):
                text_to_classify = dataset_title + "," + dataset_description
                for keyword in dataset_keywords:
                    text_to_classify = text_to_classify + "," + keyword
                for topic in dataset_topics:
                    text_to_classify = text_to_classify + "," + topic
                start_time = time.time()
                res = classifier(text_to_classify, tags)
                end_time = time.time()
                print("label: " + str(res['labels'][0]))
                print("pro: " + str(res['scores'][0]))
                file = open("DeBERTa-v3-base-mnli-xnli.txt", "a")
                file.write(filename + "\t" + str(res['labels'][0]) + "\t" + str(res['scores'][0]) + "\t" + str(end_time - start_time) + "\n")
                file.close()
            else:
                raise ValueError('oops!')
                
            contact_points =  root.xpath(config['xml']['xpath']['contactpoints']['party'], namespaces=nsmap)
            dataset_contact_points = []
            for party in contact_points:
                contactpoint = {}
                organization_names = party.xpath(config['xml']['xpath']['contactpoints']['name'], namespaces=nsmap)
                if (len(organization_names) and (organization_names[0].text is not None)):
                    contactpoint['name'] = organization_names[0].text
                contact_infos = party.xpath(config['xml']['xpath']['contactpoints']['contactinfo']['path'], namespaces=nsmap)
                if (len(contact_infos)):
                    contact_info = contact_infos[0]
                    phones = contact_info.xpath(config['xml']['xpath']['contactpoints']['contactinfo']['phone']['path'], namespaces=nsmap)
                    if(len(phones)):
                        phone = phones[0]
                        telephones = phone.xpath(config['xml']['xpath']['contactpoints']['contactinfo']['phone']['telephone'], namespaces=nsmap)
                        if(len(telephones)):
                            contactpoint['telephone'] = telephones[0].text
                        faxes = phone.xpath(config['xml']['xpath']['contactpoints']['contactinfo']['phone']['fax'], namespaces=nsmap)
                        if(len(faxes)):
                            contactpoint['fax'] = faxes[0].text
                    addresses =  contact_info.xpath(config['xml']['xpath']['contactpoints']['contactinfo']['address']['path'], namespaces=nsmap)
                    if(len(addresses)):
                        address = addresses[0]
                        delivery_points = address.xpath(config['xml']['xpath']['contactpoints']['contactinfo']['address']['deliverypoint'], namespaces=nsmap)
                        if(len(delivery_points)):
                            contactpoint['street'] = delivery_points[0].text
                        cities = address.xpath(config['xml']['xpath']['contactpoints']['contactinfo']['address']['city'], namespaces=nsmap)
                        if(len(cities)):
                            contactpoint['city'] = cities[0].text
                        postalcodes = address.xpath(config['xml']['xpath']['contactpoints']['contactinfo']['address']['postalcode'], namespaces=nsmap)
                        if(len(postalcodes)):
                            contactpoint['postalcode'] = postalcodes[0].text
                        countries = address.xpath(config['xml']['xpath']['contactpoints']['contactinfo']['address']['country'], namespaces=nsmap)
                        if(len(countries)):
                            contactpoint['country'] = countries[0].text
                        emails = address.xpath(config['xml']['xpath']['contactpoints']['contactinfo']['address']['email'], namespaces=nsmap)
                        if(len(emails)):
                            contactpoint['email'] = emails[0].text
                dataset_contact_points.append(contactpoint)

            contacts =  root.xpath(config['xml']['xpath']['contacts']['party'], namespaces=nsmap)
            for party in contacts:
                contact = {}
                organization_names = party.xpath(config['xml']['xpath']['contacts']['name'], namespaces=nsmap)
                if (len(organization_names) and (organization_names[0].text is not None)):
                    contact['name'] = organization_names[0].text
                contact_infos = party.xpath(config['xml']['xpath']['contacts']['contactinfo']['path'], namespaces=nsmap)
                if (len(contact_infos)):
                    contact_info = contact_infos[0]
                    phones = contact_info.xpath(config['xml']['xpath']['contacts']['contactinfo']['phone']['path'], namespaces=nsmap)
                    if(len(phones)):
                        phone = phones[0]
                        telephones = phone.xpath(config['xml']['xpath']['contacts']['contactinfo']['phone']['telephone'], namespaces=nsmap)
                        if(len(telephones)):
                            contact['telephone'] = telephones[0].text
                        faxes = phone.xpath(config['xml']['xpath']['contacts']['contactinfo']['phone']['fax'], namespaces=nsmap)
                        if(len(faxes)):
                            contact['fax'] = faxes[0].text
                    addresses =  contact_info.xpath(config['xml']['xpath']['contacts']['contactinfo']['address']['path'], namespaces=nsmap)
                    if(len(addresses)):
                        address = addresses[0]
                        deliverypoints = address.xpath(config['xml']['xpath']['contacts']['contactinfo']['address']['deliverypoint'], namespaces=nsmap)
                        if(len(deliverypoints)):
                            contact['street'] = deliverypoints[0].text
                        cities = address.xpath(config['xml']['xpath']['contacts']['contactinfo']['address']['city'], namespaces=nsmap)
                        if(len(cities)):
                            contact['city'] = cities[0].text
                        postalcodes = address.xpath(config['xml']['xpath']['contacts']['contactinfo']['address']['postalcode'], namespaces=nsmap)
                        if(len(postalcodes)):
                            contact['postalcode'] = postalcodes[0].text
                        countries = address.xpath(config['xml']['xpath']['contacts']['contactinfo']['address']['country'], namespaces=nsmap)
                        if(len(countries)):
                            contact['country'] = countries[0].text
                        emails = address.xpath(config['xml']['xpath']['contacts']['contactinfo']['address']['email'], namespaces=nsmap)
                        if(len(emails)):
                            contact['email'] = emails[0].text
                dataset_contact_points.append(contact)
            
            # print(dataset_contact_points)

            

            responsible_parties =  root.xpath(config['xml']['xpath']['distributors']['party'], namespaces=nsmap)
            dataset_publishers = []
            for party in responsible_parties:
                organization_names = party.xpath(config['xml']['xpath']['distributors']['name'], namespaces=nsmap)
                if (len(organization_names) and (organization_names[0].text is not None)):
                    dataset_publishers.append(organization_names[0].text)

            dataset_publishers = list(set(dataset_publishers))
            #print(dataset_publishers)

            dataset_license_name = ""
            dataset_license_type_name = ""
            dataset_license_type_code = ""
            legal_constraints = root.xpath(config['xml']['xpath']['license']['legalconstraint'], namespaces=nsmap)
            # print("Length legal contstraint" + str(len(legal_constraints)))
            for legal_constraint in legal_constraints:
                legal_constraint_name =  legal_constraint.xpath(config['xml']['xpath']['license']['name'], namespaces=nsmap)
                # print("Length constraint name" + str(len(legal_constraint_name)))
                if(len(legal_constraint_name) and (legal_constraint_name[0].text is not None)):
                    dataset_license_name = legal_constraint_name[0].text
                    # print(dataset_license_name)
                legal_constraint_type = legal_constraint.xpath(config['xml']['xpath']['license']['type']['path'], namespaces=nsmap)
                if(len(legal_constraint_type) and (legal_constraint_type[0].xpath(config['xml']['xpath']['license']['type']['name'])[0] is not None) and (legal_constraint_type[0].xpath(config['xml']['xpath']['license']['type']['code'])[0] is not None)):
                    dataset_license_type_name = legal_constraint_type[0].xpath(config['xml']['xpath']['license']['type']['name'])[0]
                    dataset_license_type_code = legal_constraint_type[0].xpath(config['xml']['xpath']['license']['type']['code'])[0]
                    if(dataset_license_type_code == "https://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_RestrictionCode" or dataset_license_type_code == "MD_RestrictionCode"):
                            dataset_license_type_code = "http://standards.iso.org/iso/19139/resources/gmxCodelists.xml#MD_RestrictionCode_" + dataset_license_type_name
                    else:
                        if (not urlparse(dataset_license_type_code)[0]):
                            dataset_license_type_code = "http://" + dataset_license_type_code
            # time.sleep(5)

            distributions =  root.xpath(config['xml']['xpath']['distributions']['path'], namespaces=nsmap)
            dataset_distributions = []
            for distribution in distributions:
                distri = {}
                dist_formats = distribution.xpath(config['xml']['xpath']['distributions']['format'], namespaces=nsmap)
                if (len(dist_formats) and (dist_formats[0].text is not None)):
                    distri['format'] = dist_formats[0].text
                else:
                    dist_formats = distribution.xpath(config['xml']['xpath']['distributions']['formatanchor'], namespaces=nsmap)
                    if (len(dist_formats) and (dist_formats[0].text is not None)):
                        distri['format'] = dist_formats[0].text 
                transfers = distribution.xpath(config['xml']['xpath']['distributions']['transfer']['path'], namespaces=nsmap)
                dist_access_urls = []
                for transfer in transfers:
                    access_urls = transfer.xpath(config['xml']['xpath']['distributions']['transfer']['accessURL'], namespaces=nsmap)
                    for access_url in access_urls:
                        if(access_url.text is not None):
                            a_url = access_url.text
                            if (not urlparse(a_url)[0]):
                                a_url = "http://" + a_url
                            # print(access_url.text)
                            # a_url = a_url.replace("[", "%5B").replace("]", "%5D").replace(":","%3A")
                            a_url = urls.url_fix(a_url)
                            dist_access_urls.append(a_url)
                distri['accessURL'] = dist_access_urls
            dataset_distributions.append(distri)

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
                themeURI = URIRef(requests.utils.requote_uri(key))
                g.add((dataset, DCAT.theme, themeURI))
                g.add((themeURI, RDF.type, SKOS.Concept))
                list_labels = dataset_themes[key]
                for i, label in enumerate(list_labels):
                    if(i == 0):
                        g.add((themeURI, SKOS.prefLabel, Literal(label, lang=iso_639_1)))
                    else:
                        g.add((themeURI, SKOS.altLabel, Literal(label)))
            for topic in dataset_topics:
                hash_object = hashlib.sha1(topic.encode())
                themeURI = URIRef(requests.utils.requote_uri(config['rdf']['uri']['topic'] + hash_object.hexdigest()))
                g.add((dataset, DCAT.theme, themeURI))
                g.add((themeURI, RDF.type, SKOS.Concept))
                g.add((themeURI, SKOS.prefLabel, Literal(topic, lang="en")))
            g.add((dataset, DCTERMS.issued, Literal(dataset_issued)))
            g.add((dataset, DCTERMS.description, Literal(dataset_description)))
            # g.add((dataset, HVD.keyword, Literal(dataset_assigned_label)))
            for contact_point in dataset_contact_points:
                if('name' in contact_point):
                    hash_object = hashlib.sha1(contact_point['name'].encode())
                    contact_pointURI = URIRef(config['rdf']['uri']['contactpoint'] + hash_object.hexdigest())
                    g.add((dataset, DCAT.contactPoint, contact_pointURI))
                    g.add((contact_pointURI, RDF.type, VCARD.Kind))
                    g.add((contact_pointURI, VCARD.title, Literal(contact_point['name'])))
                    if('telephone' in contact_point and contact_point['telephone'] is not None):
                        hash_object = hashlib.sha1(contact_point['telephone'].encode())
                        telephoneURI = URIRef(config['rdf']['uri']['telephone'] + hash_object.hexdigest())
                        g.add((telephoneURI, RDF.type, VCARD.Voice))
                        g.add((telephoneURI, VCARD.hasValue, Literal(contact_point['telephone'])))
                        g.add((contact_pointURI, VCARD.hasTelephone, telephoneURI))
                    if('fax' in contact_point and contact_point['fax'] is not None):
                        hash_object = hashlib.sha1(contact_point['fax'].encode())
                        faxURI = URIRef(config['rdf']['uri']['fax'] + hash_object.hexdigest())
                        g.add((faxURI, RDF.type, VCARD.Fax))
                        g.add((faxURI, VCARD.hasValue, Literal(contact_point['fax'])))
                        g.add((contact_pointURI, VCARD.hasTelephone, faxURI))
                    if('email' in contact_point and contact_point['email'] is not None):
                        g.add((contact_pointURI, VCARD.hasEmail, Literal(contact_point['email'])))
                    if(('street' in contact_point) or ('city' in contact_point) or ('postalcode' in contact_point) or ('country' in contact_point)):
                        address = ""
                        if('street' in contact_point and contact_point['street'] is not None):
                            address += contact_point['street']
                        if('city' in contact_point and contact_point['city'] is not None):
                            address += contact_point['city']
                        if('postalcode' in contact_point and contact_point['postalcode'] is not None):
                            address += contact_point['postalcode']
                        if('country' in contact_point and contact_point['country'] is not None):
                            address += contact_point['country']
                        hash_object = hashlib.sha1(address.encode())
                        addressURI= URIRef(config['rdf']['uri']['address'] + hash_object.hexdigest())
                        g.add((contact_pointURI, VCARD.hasAddress, addressURI))
                        g.add((addressURI, RDF.type, VCARD.Work))
                        if('street' in contact_point and contact_point['street'] is not None):
                            g.add((addressURI, VCARD['street-address'], Literal(contact_point['street'])))
                        if('city' in contact_point and contact_point['city'] is not None):
                            g.add((addressURI, VCARD.locality, Literal(contact_point['city'])))
                        if('postalcode' in contact_point and contact_point['postalcode'] is not None):
                            g.add((addressURI, VCARD['postal-code'], Literal(contact_point['postalcode'])))
                        if('country' in contact_point and contact_point['country'] is not None):
                            g.add((addressURI, VCARD['country-name'], Literal(contact_point['country'])))    
            
            for i, publisher in enumerate(dataset_publishers):
                if(i == 0): # limit to make it DCAT-AP 2.0 compliant
                    hash_object = hashlib.sha1(publisher.encode())
                    publisherURI = URIRef(config['rdf']['uri']['publisher'] + hash_object.hexdigest())
                    g.add((dataset, DCTERMS.publisher, publisherURI))
                    g.add((publisherURI, RDF.type, FOAF.Agent))
                    g.add((publisherURI, FOAF.name, Literal(publisher)))
            if(len(dataset_license_name)):
                hash_object = hashlib.sha1(dataset_license_name.encode())
                licenseURI = URIRef(config['rdf']['uri']['license'] + hash_object.hexdigest())
                g.add((licenseURI, RDF.type, DCTERMS.LicenseDocument))
                g.add((licenseURI, DCTERMS.title, Literal(dataset_license_name)))
                if(len(dataset_license_type_code)):
                    licensetypeURI = URIRef(requests.utils.requote_uri(dataset_license_type_code))
                    g.add((licensetypeURI, RDF.type, SKOS.Concept))
                    g.add((licensetypeURI, SKOS.prefLabel, Literal(dataset_license_type_name, lang="en")))
                    g.add((licenseURI, DCTERMS.type, URIRef(licensetypeURI)))
                g.add((dataset, DCTERMS.license, licenseURI))
            
            languageURI = URIRef(config['rdf']['uri']['language'] + dataset_language.upper())
            g.add((languageURI, RDF.type, DCTERMS.LinguisticSystem))
            g.add((dataset, DCTERMS.language, languageURI))

            for distribution in dataset_distributions:
                distribution_access_urls = distribution.get("accessURL")
                distribution_format = distribution.get("format")
                if(len(distribution_access_urls)):
                    access_text = ""
                    for access_url in distribution_access_urls:
                        access_text = access_text  + access_url
                    
                    # print("access text: " + access_text)
                    hash_object = hashlib.sha1(access_text.encode())
                    distributionURI = URIRef(config['rdf']['uri']['distribution'] + dataset_identifier + "/" + hash_object.hexdigest())
                    g.add((distributionURI, RDF.type, DCAT.Distribution))
                    g.add((dataset, DCAT.distribution, distributionURI))
                    for access_url in distribution_access_urls:
                        # print("accessl url: " + access_url)
                        g.add((distributionURI, DCAT.accessURL, URIRef(requests.utils.requote_uri(access_url))))
                    if(distribution_format is not None):
                        # print("format: " + distribution_format)
                        # distribution_format = distribution_format.replace("[", "%5B").replace("]","%5D")
                        distribution_format = urls.url_fix(distribution_format)
                        format_uri = requests.utils.requote_uri(config['rdf']['uri']['format'] + distribution_format.upper())
                        g.add((distributionURI, DCTERMS.format, URIRef(format_uri)))
                        g.add((URIRef(format_uri), RDF.type, DCTERMS.MediaTypeOrExtent))


    
            # print(config['message']['export'])
            outputfile = config['output']['folder'] + "/" + namefile + ".nt"
            g.serialize(destination=outputfile, format=config['output']['format'],encoding='utf-8')
            g.close()

# def convert(filename, labels):
def convert(filename):
    config = get_config('config-convert.yaml')
    # tokenizer = AutoTokenizer.from_pretrained('facebook/bart-large-mnli')
    
    tags = config['labels']
    directory = config['input']['folder']
    f = os.path.join(directory, filename)
    # checking if it is a file
    if os.path.isfile(f) and os.path.splitext(f)[-1].lower() == config['input']['extension']:
        # print(f)

        g = Graph()
        VCARD = Namespace(config['rdf']['vcard']['uri'])
        namespace = config['rdf']['vcard']['namespace']
        g.bind(namespace, VCARD)

        HVD = Namespace(config['rdf']['hvd']['uri'])
        namespace = config['rdf']['hvd']['namespace']
        g.bind(namespace, HVD)

        namefile = (Path(f).stem)
        dataset_location =  namefile.split("-")[1]    
        dataset_identifier = namefile.replace(" ","_")
        #print(dataset_identifier)

        # file_bar.set_description(namefile)
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

        language_code = root.xpath(config['xml']['xpath']['language']['code'], namespaces=nsmap)
        language_2_code = root.xpath(config['xml']['xpath']['language2'], namespaces=nsmap)
        dataset_language = ""
        if(language_code):
            dataset_language = language_code[0].xpath(config['xml']['xpath']['language']['value'], namespaces=nsmap)[0]
        if(language_2_code):
            dataset_language = language_2_code[0].text
        iso_639_1 = ""
        if(len(dataset_language) and dataset_language != "zxx"):
            iso_639_1 = config['map']['languages'][dataset_language]
        
        title = root.xpath(config['xml']['xpath']['title'], namespaces=nsmap)
        dataset_title = ""
        if (title):
            dataset_title = title[0].text
        else:
            titles = root.xpath(config['xml']['xpath']['titlefreetext'], namespaces=nsmap)
            for title in titles:
                if iso_639_1.upper() in title.xpath(config['xml']['xpath']['titlefreetextlanguage'], namespaces=nsmap)[0]:
                    dataset_title = title.text
        #print(dataset_title)

        dataset_keywords = []
        keywords = root.xpath(config['xml']['xpath']['keywords'], namespaces=nsmap)
        for keyword in keywords:
            if(keyword.text is not None):
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

        topics = root.xpath(config['xml']['xpath']['topic'], namespaces=nsmap)
        dataset_topics = []
        for topic in topics:
            topic_text = topic.text
            if((topic_text is not None) and len(topic_text)):
                    print("topic: " + topic_text)
                    dataset_topics.append(topic_text)

        issued_date = root.xpath(config['xml']['xpath']['date'], namespaces=nsmap)
        issued_datetime = root.xpath(config['xml']['xpath']['datetime'], namespaces=nsmap)
        dataset_issued = ""
        if (len(issued_date) and len(issued_date[0].text)):
            dataset_issued = issued_date[0].text
        else:
            if (len(issued_datetime) and len(issued_datetime[0].text) ):
                dataset_issued = issued_datetime[0].text 
        
        #print(dataset_issued)
        abstract_list = root.xpath(config['xml']['xpath']['abstract'], namespaces=nsmap)
        dataset_description = ""
        for abstract in abstract_list:
                if(abstract.text):
                    dataset_description = abstract.text
        else:
            abstract2_list = root.xpath(config['xml']['xpath']['abstractfreetext'], namespaces=nsmap)
            for abstract in abstract2_list:
                if iso_639_1.upper() in abstract.xpath(config['xml']['xpath']['abstractfreetext'], namespaces=nsmap)[0]:
                    dataset_description = abstract.text
        #print(dataset_description)
        # start_time = time.time()
        # dataset_assigned_label = tags[labels(dataset_description, tags)[0][0]]
        # - res = labels(dataset_description, tags)
        # - dataset_assigned_label = res['labels'][0]
        # - print("pro: " + str(res['scores'][0]))
        # dataset_assigned_label = ""
        # end_time = time.time()
        # print("Time:", end_time - start_time)
        contact_points =  root.xpath(config['xml']['xpath']['contactpoints']['party'], namespaces=nsmap)
        dataset_contact_points = []
        for party in contact_points:
            contactpoint = {}
            organization_names = party.xpath(config['xml']['xpath']['contactpoints']['name'], namespaces=nsmap)
            if (len(organization_names) and (organization_names[0].text is not None)):
                contactpoint['name'] = organization_names[0].text
            contact_infos = party.xpath(config['xml']['xpath']['contactpoints']['contactinfo']['path'], namespaces=nsmap)
            if (len(contact_infos)):
                contact_info = contact_infos[0]
                phones = contact_info.xpath(config['xml']['xpath']['contactpoints']['contactinfo']['phone']['path'], namespaces=nsmap)
                if(len(phones)):
                    phone = phones[0]
                    telephones = phone.xpath(config['xml']['xpath']['contactpoints']['contactinfo']['phone']['telephone'], namespaces=nsmap)
                    if(len(telephones)):
                        contactpoint['telephone'] = telephones[0].text
                    faxes = phone.xpath(config['xml']['xpath']['contactpoints']['contactinfo']['phone']['fax'], namespaces=nsmap)
                    if(len(faxes)):
                        contactpoint['fax'] = faxes[0].text
                addresses =  contact_info.xpath(config['xml']['xpath']['contactpoints']['contactinfo']['address']['path'], namespaces=nsmap)
                if(len(addresses)):
                    address = addresses[0]
                    delivery_points = address.xpath(config['xml']['xpath']['contactpoints']['contactinfo']['address']['deliverypoint'], namespaces=nsmap)
                    if(len(delivery_points)):
                        contactpoint['street'] = delivery_points[0].text
                    cities = address.xpath(config['xml']['xpath']['contactpoints']['contactinfo']['address']['city'], namespaces=nsmap)
                    if(len(cities)):
                        contactpoint['city'] = cities[0].text
                    postalcodes = address.xpath(config['xml']['xpath']['contactpoints']['contactinfo']['address']['postalcode'], namespaces=nsmap)
                    if(len(postalcodes)):
                        contactpoint['postalcode'] = postalcodes[0].text
                    countries = address.xpath(config['xml']['xpath']['contactpoints']['contactinfo']['address']['country'], namespaces=nsmap)
                    if(len(countries)):
                        contactpoint['country'] = countries[0].text
                    emails = address.xpath(config['xml']['xpath']['contactpoints']['contactinfo']['address']['email'], namespaces=nsmap)
                    if(len(emails)):
                        contactpoint['email'] = emails[0].text
            dataset_contact_points.append(contactpoint)

        contacts =  root.xpath(config['xml']['xpath']['contacts']['party'], namespaces=nsmap)
        for party in contacts:
            contact = {}
            organization_names = party.xpath(config['xml']['xpath']['contacts']['name'], namespaces=nsmap)
            if (len(organization_names) and (organization_names[0].text is not None)):
                contact['name'] = organization_names[0].text
            contact_infos = party.xpath(config['xml']['xpath']['contacts']['contactinfo']['path'], namespaces=nsmap)
            if (len(contact_infos)):
                contact_info = contact_infos[0]
                phones = contact_info.xpath(config['xml']['xpath']['contacts']['contactinfo']['phone']['path'], namespaces=nsmap)
                if(len(phones)):
                    phone = phones[0]
                    telephones = phone.xpath(config['xml']['xpath']['contacts']['contactinfo']['phone']['telephone'], namespaces=nsmap)
                    if(len(telephones)):
                        contact['telephone'] = telephones[0].text
                    faxes = phone.xpath(config['xml']['xpath']['contacts']['contactinfo']['phone']['fax'], namespaces=nsmap)
                    if(len(faxes)):
                        contact['fax'] = faxes[0].text
                addresses =  contact_info.xpath(config['xml']['xpath']['contacts']['contactinfo']['address']['path'], namespaces=nsmap)
                if(len(addresses)):
                    address = addresses[0]
                    deliverypoints = address.xpath(config['xml']['xpath']['contacts']['contactinfo']['address']['deliverypoint'], namespaces=nsmap)
                    if(len(deliverypoints)):
                        contact['street'] = deliverypoints[0].text
                    cities = address.xpath(config['xml']['xpath']['contacts']['contactinfo']['address']['city'], namespaces=nsmap)
                    if(len(cities)):
                        contact['city'] = cities[0].text
                    postalcodes = address.xpath(config['xml']['xpath']['contacts']['contactinfo']['address']['postalcode'], namespaces=nsmap)
                    if(len(postalcodes)):
                        contact['postalcode'] = postalcodes[0].text
                    countries = address.xpath(config['xml']['xpath']['contacts']['contactinfo']['address']['country'], namespaces=nsmap)
                    if(len(countries)):
                        contact['country'] = countries[0].text
                    emails = address.xpath(config['xml']['xpath']['contacts']['contactinfo']['address']['email'], namespaces=nsmap)
                    if(len(emails)):
                        contact['email'] = emails[0].text
            dataset_contact_points.append(contact)
        
        # print(dataset_contact_points)

        

        responsible_parties =  root.xpath(config['xml']['xpath']['distributors']['party'], namespaces=nsmap)
        dataset_publishers = []
        for party in responsible_parties:
            organization_names = party.xpath(config['xml']['xpath']['distributors']['name'], namespaces=nsmap)
            if (len(organization_names) and (organization_names[0].text is not None)):
                dataset_publishers.append(organization_names[0].text)

        dataset_publishers = list(set(dataset_publishers))
        #print(dataset_publishers)

        dataset_license_name = ""
        dataset_license_type_name = ""
        dataset_license_type_code = ""
        legal_constraints = root.xpath(config['xml']['xpath']['license']['legalconstraint'], namespaces=nsmap)
        # print("Length legal contstraint" + str(len(legal_constraints)))
        for legal_constraint in legal_constraints:
            legal_constraint_name =  legal_constraint.xpath(config['xml']['xpath']['license']['name'], namespaces=nsmap)
            # print("Length constraint name" + str(len(legal_constraint_name)))
            if(len(legal_constraint_name) and (legal_constraint_name[0].text is not None)):
                dataset_license_name = legal_constraint_name[0].text
                # print(dataset_license_name)
            legal_constraint_type = legal_constraint.xpath(config['xml']['xpath']['license']['type']['path'], namespaces=nsmap)
            if(len(legal_constraint_type) and (legal_constraint_type[0].xpath(config['xml']['xpath']['license']['type']['name'])[0] is not None) and (legal_constraint_type[0].xpath(config['xml']['xpath']['license']['type']['code'])[0] is not None)):
                dataset_license_type_name = legal_constraint_type[0].xpath(config['xml']['xpath']['license']['type']['name'])[0]
                dataset_license_type_code = legal_constraint_type[0].xpath(config['xml']['xpath']['license']['type']['code'])[0]
        # time.sleep(5)

        distributions =  root.xpath(config['xml']['xpath']['distributions']['path'], namespaces=nsmap)
        dataset_distributions = []
        for distribution in distributions:
            distri = {}
            dist_formats = distribution.xpath(config['xml']['xpath']['distributions']['format'], namespaces=nsmap)
            if (len(dist_formats) and (dist_formats[0].text is not None)):
                distri['format'] = dist_formats[0].text
            else:
                dist_formats = distribution.xpath(config['xml']['xpath']['distributions']['formatanchor'], namespaces=nsmap)
                if (len(dist_formats) and (dist_formats[0].text is not None)):
                    distri['format'] = dist_formats[0].text 
            transfers = distribution.xpath(config['xml']['xpath']['distributions']['transfer']['path'], namespaces=nsmap)
            dist_access_urls = []
            for transfer in transfers:
                access_urls = transfer.xpath(config['xml']['xpath']['distributions']['transfer']['accessURL'], namespaces=nsmap)
                for access_url in access_urls:
                    if(access_url.text is not None):
                        #print(access_url.text)
                        dist_access_urls.append(access_url.text)
            distri['accessURL'] = dist_access_urls
            dataset_distributions.append(distri)

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
            themeURI = URIRef(requests.utils.requote_uri(key))
            g.add((dataset, DCAT.theme, themeURI))
            g.add((themeURI, RDF.type, SKOS.Concept))
            list_labels = dataset_themes[key]
            for i, label in enumerate(list_labels):
                if(i == 0):
                    g.add((themeURI, SKOS.prefLabel, Literal(label, lang=iso_639_1)))
                else:
                    g.add((themeURI, SKOS.altLabel, Literal(label)))
        for topic in dataset_topics:
            hash_object = hashlib.sha1(topic.encode())
            themeURI = URIRef(requests.utils.requote_uri(config['rdf']['uri']['topic'] + hash_object.hexdigest()))
            g.add((dataset, DCAT.theme, themeURI))
            g.add((themeURI, RDF.type, SKOS.Concept))
            g.add((themeURI, SKOS.prefLabel, Literal(topic, lang="en")))
        g.add((dataset, DCTERMS.issued, Literal(dataset_issued)))
        g.add((dataset, DCTERMS.description, Literal(dataset_description)))
        # g.add((dataset, HVD.keyword, Literal(dataset_assigned_label)))
        for contact_point in dataset_contact_points:
            if('name' in contact_point):
                hash_object = hashlib.sha1(contact_point['name'].encode())
                contact_pointURI = URIRef(config['rdf']['uri']['contactpoint'] + hash_object.hexdigest())
                g.add((dataset, DCAT.contactPoint, contact_pointURI))
                g.add((contact_pointURI, RDF.type, VCARD.Kind))
                g.add((contact_pointURI, VCARD.title, Literal(contact_point['name'])))
                if('telephone' in contact_point and contact_point['telephone'] is not None):
                    hash_object = hashlib.sha1(contact_point['telephone'].encode())
                    telephoneURI = URIRef(config['rdf']['uri']['telephone'] + hash_object.hexdigest())
                    g.add((telephoneURI, RDF.type, VCARD.Voice))
                    g.add((telephoneURI, VCARD.hasValue, Literal(contact_point['telephone'])))
                    g.add((contact_pointURI, VCARD.hasTelephone, telephoneURI))
                if('fax' in contact_point and contact_point['fax'] is not None):
                    hash_object = hashlib.sha1(contact_point['fax'].encode())
                    faxURI = URIRef(config['rdf']['uri']['fax'] + hash_object.hexdigest())
                    g.add((faxURI, RDF.type, VCARD.Fax))
                    g.add((faxURI, VCARD.hasValue, Literal(contact_point['fax'])))
                    g.add((contact_pointURI, VCARD.hasTelephone, faxURI))
                if('email' in contact_point and contact_point['email'] is not None):
                    g.add((contact_pointURI, VCARD.hasEmail, Literal(contact_point['email'])))
                if(('street' in contact_point) or ('city' in contact_point) or ('postalcode' in contact_point) or ('country' in contact_point)):
                    address = ""
                    if('street' in contact_point and contact_point['street'] is not None):
                        address += contact_point['street']
                    if('city' in contact_point and contact_point['city'] is not None):
                        address += contact_point['city']
                    if('postalcode' in contact_point and contact_point['postalcode'] is not None):
                        address += contact_point['postalcode']
                    if('country' in contact_point and contact_point['country'] is not None):
                        address += contact_point['country']
                    hash_object = hashlib.sha1(address.encode())
                    addressURI= URIRef(config['rdf']['uri']['address'] + hash_object.hexdigest())
                    g.add((contact_pointURI, VCARD.hasAddress, addressURI))
                    g.add((addressURI, RDF.type, VCARD.Work))
                    if('street' in contact_point and contact_point['street'] is not None):
                        g.add((addressURI, VCARD['street-address'], Literal(contact_point['street'])))
                    if('city' in contact_point and contact_point['city'] is not None):
                        g.add((addressURI, VCARD.locality, Literal(contact_point['city'])))
                    if('postalcode' in contact_point and contact_point['postalcode'] is not None):
                        g.add((addressURI, VCARD['postal-code'], Literal(contact_point['postalcode'])))
                    if('country' in contact_point and contact_point['country'] is not None):
                        g.add((addressURI, VCARD['country-name'], Literal(contact_point['country'])))    
        
        for i, publisher in enumerate(dataset_publishers):
            if(i == 0): # limit to make it DCAT-AP 2.0 compliant
                hash_object = hashlib.sha1(publisher.encode())
                publisherURI = URIRef(config['rdf']['uri']['publisher'] + hash_object.hexdigest())
                g.add((dataset, DCTERMS.publisher, publisherURI))
                g.add((publisherURI, RDF.type, FOAF.Agent))
                g.add((publisherURI, FOAF.name, Literal(publisher)))
        if(len(dataset_license_name)):
            hash_object = hashlib.sha1(dataset_license_name.encode())
            licenseURI = URIRef(config['rdf']['uri']['license'] + hash_object.hexdigest())
            g.add((licenseURI, RDF.type, DCTERMS.LicenseDocument))
            g.add((licenseURI, DCTERMS.title, Literal(dataset_license_name)))
            if(len(dataset_license_type_code)):
                licensetypeURI = URIRef(requests.utils.requote_uri(dataset_license_type_code))
                g.add((licensetypeURI, RDF.type, SKOS.Concept))
                g.add((licensetypeURI, SKOS.prefLabel, Literal(dataset_license_type_name, lang="en")))
                g.add((licenseURI, DCTERMS.type, URIRef(licensetypeURI)))
            g.add((dataset, DCTERMS.license, licenseURI))
        
        languageURI = URIRef(config['rdf']['uri']['language'] + dataset_language.upper())
        g.add((languageURI, RDF.type, DCTERMS.LinguisticSystem))
        g.add((dataset, DCTERMS.language, languageURI))

        for distribution in dataset_distributions:
            distribution_access_urls = distribution.get("accessURL")
            distribution_format = distribution.get("format")
            if(len(distribution_access_urls)):
                access_text = ""
                for access_url in distribution_access_urls:
                    access_text = access_text  + access_url
                
                # print("access text: " + access_text)
                hash_object = hashlib.sha1(access_text.encode())
                distributionURI = URIRef(config['rdf']['uri']['distribution'] + dataset_identifier + "/" + hash_object.hexdigest())
                g.add((distributionURI, RDF.type, DCAT.Distribution))
                g.add((dataset, DCAT.distribution, distributionURI))
                for access_url in distribution_access_urls:
                    # print("accessl url: " + access_url)
                    g.add((distributionURI, DCAT.accessURL, URIRef(requests.utils.requote_uri(access_url))))
                if(distribution_format is not None):
                    # print("format: " + distribution_format)
                    disallowed_characters = "[]"
                    for character in disallowed_characters:
                        distribution_format = distribution_format.replace(character, "")
                    format_uri = requests.utils.requote_uri(config['rdf']['uri']['format'] + distribution_format.upper())
                    g.add((distributionURI, DCTERMS.format, URIRef(format_uri)))
                    g.add((URIRef(format_uri), RDF.type, DCTERMS.MediaTypeOrExtent))



        # print(config['message']['export'])
        outputfile = config['output']['folder'] + "/" + namefile + ".nt"
        g.serialize(destination=outputfile, format=config['output']['format'],encoding='utf-8')
        g.close()

def validate(config):
    print(config['message']['validation'])
    # outputfile = config['output']['folder'] + "/" + config['output']['file']

    directory = config['output']['folder']
    filelist = os.listdir(directory)

    file_bar = tqdm(filelist, desc=config['bar']['files']['description'], colour=config['bar']['files']['colour'], leave=config['bar']['files']['leave'])

    for index, filename in enumerate(file_bar):
        if(index % 300 == 0):
            file_bar.set_description(filename)
            txt = Path(directory + "/" + filename).read_text(encoding='utf-8')
            myobj = { "contentSyntax": config['validation']['inputsyntax'], 
                    "contentToValidate": txt, 
                    "validationType" : config['validation']['version'], 
                    "reportSyntax": config['validation']['outputsyntax'] }

            url = config['validation']['url']
            response = requests.post(url, json=myobj)
            # print("Status Code", response.status_code)

            result_text_json = response.text
            my_json= json.loads(result_text_json)
            if(my_json.get(config['validation']['jsonconform'])):
                print(config['message']['conform'])
            else:
                f = open(config['log']['folder'] + "/" + os.path.splitext(filename)[0] + config['log']['extension'], "w")
                f.write(result_text_json)
                f.close()
                print(config['message']['notconform'])
                print(result_text_json)

def merge(config):
    if not os.listdir(config['log']['folder']) :
        directory = config['output']['folder']
        filelist = os.listdir(directory)
        pathfiles = [os.path.join(directory, f) for f in filelist]
        with open(config['output']['folder'] + "/" + config['output']['outputfile'],'wb') as wfd:
            merge_bar = tqdm(pathfiles, desc=config['bar']['merge']['description'], colour=config['bar']['merge']['colour'], leave=config['bar']['merge']['leave'])
            for f in merge_bar:
                with open(f,'rb') as fd:
                    shutil.copyfileobj(fd, wfd)
                    wfd.write(b"\n")
    else:
        print(config['message']['error'] )

from functools import partial


def multip():
    config = get_config('config-convert.yaml')
    converter(config)
    
    directory = config['input']['folder']
    filelist = os.listdir(directory)

    # prod_x=partial(convert, y=10)
    # model =  AutoModel.from_pretrained('valhalla/distilbart-mnli-12-3')
    # tokenizer = AutoTokenizer.from_pretrained('valhalla/distilbart-mnli-12-3')
    # classifier = pipeline(task='zero-shot-classification', model='valhalla/distilbart-mnli-12-3')
    
    # labels = Labels(model = model)
    # classifier = pipeline('zero-shot-classification',model='joeddav/xlm-roberta-large-xnli')

    # prod_x=partial(convert, labels=classifier)
    '''
    prod_x=partial(convert)
    with Pool(processes=3) as pool, tqdm(total=len(filelist), desc=config['bar']['files']['description'], colour=config['bar']['files']['colour'], leave=config['bar']['files']['leave']) as pbar: # create Pool of processes (only 2 in this example) and tqdm Progress bar                                                     # into this list I will store the urls returned from parse() function
            for data in pool.imap_unordered(prod_x, filelist):                   # send urls from all_urls list to parse() function (it will be done concurently in process pool). The results returned will be unordered (returned when they are available, without waiting for other processes)
                pbar.update() 
        
    pool.close()
    pool.join()
    '''
    validate(config)
    merge(config)

if __name__ == '__main__':
     multip()
