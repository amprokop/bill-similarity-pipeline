# python
import json
import pickle
import string

import os
from os import path, listdir
from os.path import join
from pathlib import Path
 
from sklearn.feature_extraction.text import TfidfVectorizer
from nltk.tokenize import RegexpTokenizer
import prefect
from prefect import task, Flow, Parameter
from prefect.executors import LocalDaskExecutor

from utils import xml_to_sections, text_cleaning, getHeader, getEnum
from constants import PATH_116_USLM, PATH_117_USLM, NAMESPACES, BILL_CORPUS_FILE, SECTION_CORPUS_FILE, BILL_VECTOR_FILE, SECTION_VECTOR_FILE

# Flow to 



# Accepts array of directories containing USLM bills (116th, congress, 117th congress etc) 
# Returns file paths 
@task(log_stdout=True)
def get_bill_file_paths(dirs):
    bill_files = []
    print('Finding bill files')

    for d in dirs:
        if os.path.isdir(d) == False:
            print("Bill directory not found")

        for f in os.listdir(d):
            if f.endswith('.xml'):
                print(f)
                bill_files.append(os.path.join(d, f))
    
    print(f'{len(bill_files)} bill files found')
    return bill_files

# Accepts list of fully qualified USLM bill paths
# Returns 
@task(log_stdout=True)
def extract_transform_load_bills(bill_files): 
    print('Beginning ETL step')

    doc_corpus_data=[]
    section_corpus_data = []

    for i in range(0, len(bill_files)):
        bill_doc_file = bill_files[i]
        #parse xml into sections
        secs = xml_to_sections(bill_doc_file)

        if(len(secs)>0):  
            #intialize string variable for document content
            doc_content = ""
            #iterate over all parse sections text of bill doc file
            for s_number, section in enumerate(secs):  
                #text cleaning applied on each section text
                sec_text = text_cleaning(section['section_text'])
                #concatenate section text to doc content 
                doc_content = doc_content + sec_text + " "
                #for now sentence id is sentence number in document
                section_corpus_data.append([Path(bill_doc_file).stem[:], s_number, sec_text ])
            doc_corpus_data.append([Path(bill_doc_file).stem[:], doc_content])
        else:
            print("No sections found")

    #get only whole document content from doc_corpus_data list
    only_doc_data = [row[1] for row in doc_corpus_data]
    #get only section content from section_corpus_data list
    only_section_data = [row[2] for row in section_corpus_data]

    #store pre-processed document corpus and section level corpus
    # todo: index bill text in elasticsearch instead? 
    pickle.dump(doc_corpus_data, open(BILL_CORPUS_FILE, "wb"))
    pickle.dump(section_corpus_data, open(SECTION_CORPUS_FILE, "wb"))
    #get length of only_doc_data list
    print(f'{len(only_doc_data)} documents found in corpus')
    print(f'{len(only_section_data)} sections found in corpus')

    return only_doc_data, only_section_data
   
@task(log_stdout=True)
def vectorize_corpus(corpus_data, output_filename):
    tfidf_vectorizer = TfidfVectorizer(ngram_range=(4,4), tokenizer=RegexpTokenizer(r"\w+").tokenize, lowercase=True)
    tv_section_matrix = tfidf_vectorizer.fit_transform(corpus_data)
    pickle.dump(tfidf_vectorizer, open(output_filename, "wb"))

    return tfidf_vectorizer

with Flow("vectorize_bills", executor=LocalDaskExecutor()) as flow:
    file_paths = get_bill_file_paths([PATH_117_USLM, PATH_116_USLM])
    bill_data = extract_transform_load_bills(file_paths)
    doc_vectors = vectorize_corpus(bill_data[0], BILL_VECTOR_FILE)
    section_vectors = vectorize_corpus(bill_data[1], SECTION_VECTOR_FILE)
    
flow.register(project_name="BillSimilarityEngine")