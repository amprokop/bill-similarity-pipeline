import json
import time
import pickle
import re, string
import os
from os import path, listdir
from os.path import isfile, join
from pathlib import Path
from lxml import etree 
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from nltk.tokenize import RegexpTokenizer
import prefect
from prefect import task, Flow, Parameter
from prefect.executors import LocalDaskExecutor
from billsim import utils_db, pymodels
from sqlalchemy.orm import sessionmaker
from sqlmodel import create_engine

from utils import xml_to_sections, text_cleaning, getHeader, getEnum
from constants import PATH_116_USLM, PATH_117_USLM, NAMESPACES, BILL_CORPUS_FILE, SECTION_CORPUS_FILE, BILL_VECTOR_FILE, SECTION_VECTOR_FILE
# Flow to simulate taking in a large new bill and generating comparisons.
#
# Given one bill, vectorize that one bill, run bill-level cosine similarity against 30 other bills, 
# save the results, and time how long it takes.
# 
# Assumes the existence of a doc corpus and vector corpus file. 
# Does not add the "new" bill to either the vector corpus or doc corpus.
# 
# Right now, we'll hardcode 116hr6395enr, which is the version of the 2020 NDAA with the largest filesize (12+MB).
# We'll compare it to a hardcoded list of 30 bills, some of which are other versions of the NDAA, some of which are randomly chosen.

PATH_TO_BILL = '../xc-nlp-test/samples/congress/116/uslm/BILLS-116hr6395enr.xml'
BILLNUMBER_VERSION = '116hr6395enr'

# compute cosine pairwise similarity
def cosine_pairwise_sim(a_vectorized, b_vectorized):
    
    #record time for computing similarity 
    start = time.time()

    sim_score =  cosine_similarity(a_vectorized, b_vectorized)

    done = time.time()
    elapsed = done - start
    return elapsed, sim_score

@task(log_stdout=True)
def get_new_bill_file_path():
    return PATH_TO_BILL

@task(log_stdout=True)
def get_bill_number_versions_to_compare():
    return ["116hr6395eh",
    "116hr133enr",
    "116hr133eah",
    "116hr6395pcs",
    "116hr7095ih",
    "116s47es",
    "116hr1893ih",
    "116hr265ih",
    "116s468is",
    "116hr6646ih",
    "116s269is",
    "116hr6741ih",
    "116hr6056ih",
    "116hr6807ih",
    "116hr5548ih",
    "116hr6116ih",
    "117hr1319eh",
    "117s2238is",
    "117hr2780ih",
    "117hr3165ih",
    "117hr1227ih",
    "117hr4289ih",
    "117s1185is",
    "117hr4436ih",
    "117hr4681ih",
    "117hr2941ih",
    "117hr878ih",
    "117s1737is",
    "117hr4237ih",
    "117hr4220ih"]

@task(log_stdout=True)
def process_new_bill_text(bill_doc_path):
    secs = xml_to_sections(bill_doc_path)

    if(len(secs)==0): 
        print("No sections found")

    #intialize string variable for document content
    doc_content = ""
    #iterate over all parse sections text of bill doc file
    for s_number, section in enumerate(secs):  
        #text cleaning applied on each section text
        sec_text = text_cleaning(section['section_text'])
        #concatenate section text to doc content 
        doc_content = doc_content + sec_text + " "
        #for now sentence id is sentence number in document

    return doc_content

@task(log_stdout=True)
def fetch_bill_text_from_corpus(billnumber_versions):
    # billnumber_versions ~ ["116hr435inh", "115hr434enh"]
    doc_corpus_data = open(BILL_CORPUS_FILE, "rb")
    doc_corpus_data = pickle.load(doc_corpus_data)
    
    bill_text = []
    for billnumber_version in billnumber_versions: 
        doc_name = "BILLS-" + billnumber_version
    
        try:
            doc = [i[1] for i in doc_corpus_data if doc_name == i[0]][0]
            bill_text.append({"billnumber_version":billnumber_version, "bill_text": doc})
        except IndexError:
            print('Could not find bill in corpus. Skipping: ', billnumber_version)
            continue
    
    return bill_text


# billnumber_version: "117hr323eh"
# billtext: cleaned processed flat bill_text
# billnumber_versions_and_texts_to_compare:
# [{"billnumber_version" : "116s323enr", "bill_text" : cleaned processed flat bill text }]
@task(log_stdout=True)
def calculate_bill_similarity(billnumber_version, bill_text, billnumber_versions_and_texts_to_compare):
    doc_tfidf_vectorizer = open(BILL_VECTOR_FILE, "rb")
    doc_tfidf_vectorizer = pickle.load(doc_tfidf_vectorizer)

    A_doc_vectorized = doc_tfidf_vectorizer.transform([bill_text])

    for billnumber_version_and_text in billnumber_versions_and_texts_to_compare:
        text_to_compare = billnumber_version_and_text["bill_text"]
        billnumber_version_to_compare = billnumber_version_and_text["billnumber_version"]

        B_doc_vectorized = doc_tfidf_vectorizer.transform([text_to_compare])

        elapsed_1, from_doc_similarity = cosine_pairwise_sim(A_doc_vectorized, B_doc_vectorized)

        # todo: we can make this one db call instead of N
        ids = utils_db.get_bill_ids(billnumber_versions=[billnumber_version, billnumber_version_to_compare])

        print("Bill: ", billnumber_version, " Bill_to: ", billnumber_version_to_compare,  " Doc similarity: ", from_doc_similarity[0][0])
        if len(ids) == 2 and ids[billnumber_version] != ids[billnumber_version_to_compare]:
            print("Both bills found. Saving.")
            print("Doc similarity: ", from_doc_similarity[0][0])

            # TODO: do we save 2 bill to bill records for each comparison?
            bill_to_bill_new = pymodels.BillToBillModel(
                billnumber_version= billnumber_version,
                billnumber_version_to= billnumber_version_to_compare,
                score= from_doc_similarity[0][0],
                bill_id= ids[billnumber_version],
                bill_to_id= ids[billnumber_version_to_compare]
            )

            btb = utils_db.save_bill_to_bill(bill_to_bill_model= bill_to_bill_new)
        else:
            print("Bills not found, or bill A & B are identical")



with Flow("process_new_bill", executor=LocalDaskExecutor()) as flow:
    new_bill_path = get_new_bill_file_path()
    bill_text = process_new_bill_text(new_bill_path)
    bills_to_compare = get_bill_number_versions_to_compare()
    bills_to_compare_obj = fetch_bill_text_from_corpus(bills_to_compare)
    calculate_bill_similarity(BILLNUMBER_VERSION, bill_text, bills_to_compare_obj)

flow.register(project_name="BillSimilarityEngine")