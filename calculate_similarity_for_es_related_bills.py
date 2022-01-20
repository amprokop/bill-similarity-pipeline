# flow that parallelizes bill-level comparisons

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

from constants import PATH_116_USLM, PATH_117_USLM, NAMESPACES, BILL_CORPUS_FILE, SECTION_CORPUS_FILE, BILL_VECTOR_FILE, SECTION_VECTOR_FILE

# Flow to query the Billsim database for any bills with ES scores,
# and calculate cosine similarity for the top 30 ES scoring bills.
# Assumes the existence of vector and corpus files.

@task(log_stdout=True)
def get_bills_to_compare():
    postgres_url = f"postgresql://postgres:postgres@localhost:5433"

    engine = create_engine(postgres_url, echo=False)

    SessionLocal = sessionmaker(autocommit=False,
                                autoflush=False,
                                expire_on_commit=False,
                            bind=engine)

    query = ("SELECT billtobill.bill_id, billtobill.bill_to_id, billtobill.score_es, " 
        "bill1.billnumber AS bill1number, bill1.version AS bill1version, "
        "bill2.billnumber as bill2number, bill2.version AS bill2version " 
        "FROM billtobill "
        "LEFT JOIN bill AS bill1 ON billtobill.bill_id = bill1.id "
        "LEFT JOIN bill AS bill2 ON billtobill.bill_to_id = bill2.id "
        "WHERE billtobill.score_es IS NOT NULL " 
        "ORDER BY billtobill.bill_id ASC, billtobill.score_es DESC;")

    rs = SessionLocal().execute(query)
    results = rs.fetchall()

    print("Number of records with value for ES score: ", len(results))
    # group by bill_id
    bill_data_obj = {}
    for result in results:
        score_es = result[2]
        bill1numberversion = result[3] + result[4]
        bill2numberversion = result[5] + result[6]

        if not bill1numberversion in bill_data_obj.keys():
            bill_data_obj[bill1numberversion] = []

        # Format results like so: { '116hr5001ih' : [['116hr50002ih', 631.2436789999999], ['117hr333enr', 43.4343]] }
        bill_data_obj[bill1numberversion].append([bill2numberversion, score_es])

    # keep only the top 30 values for es_score for each bill_id  
    bills_to_compare = []

    for bill_number_version, bill_list in bill_data_obj.items():
        if len(bill_list) < 30:
            bills_to_compare.append([ bill_number_version, bill_list ])
        else:
            bills_to_compare.append([bill_number_version, bill_list[0:30]])
    
    count = 0
    for group in bills_to_compare:
        count = count + len(group[1])

    print("Number of bills to compare: ", len(bills_to_compare))
    print("Total number of comparisons to run: ", count)
    
    return bills_to_compare

# compute cosine pairwise similarity
def cosine_pairwise_sim(a_vectorized, b_vectorized):
    
    #record time for computing similarity 
    start = time.time()

    sim_score =  cosine_similarity(a_vectorized, b_vectorized)

    done = time.time()
    elapsed = done - start
    return elapsed, sim_score


@task(log_stdout=True)
def calculate_bill_similarity(bills_to_compare):

    # expects format [["117hr433ih", ["116hr435inh", "115hrenh"]],["115hr11enr", ["117hr122inh", "117hr75ih"]]]
    doc_corpus_data = open(BILL_CORPUS_FILE, "rb")
    doc_tfidf_vectorizer = open(BILL_VECTOR_FILE, "rb")
    section_corpus_data = open(SECTION_CORPUS_FILE, "rb")
    sec_tfidf_vectorizer = open(SECTION_VECTOR_FILE, "rb")

    section_corpus_data = pickle.load(section_corpus_data)
    doc_corpus_data = pickle.load(doc_corpus_data)
    doc_tfidf_vectorizer = pickle.load(doc_tfidf_vectorizer)
    sec_tfidf_vectorizer = pickle.load(sec_tfidf_vectorizer)

    skip_count = 0

    for pair in bills_to_compare:
        bill_A = pair[0]
        bills_to_compare_list = pair[1]
        for bill_info in bills_to_compare_list:  
            bill_B = bill_info[0]
            print("Running comparison: ", bill_A, " ", bill_B)

            #pick any Document A & any Document B from data lists (at least that have more than 1 section)
            A_doc_name = "BILLS-" + bill_A
            B_doc_name = "BILLS-" + bill_B
        
            try:
                A_doc = [i[1] for i in doc_corpus_data if A_doc_name ==i[0]][0]
                B_doc = [i[1] for i in doc_corpus_data if B_doc_name ==i[0]][0]
            except IndexError:
                print('Could not find both bills in corpus. Skipping.')
                skip_count += 1
                continue

            A_doc_vectorized = doc_tfidf_vectorizer.transform([A_doc])
            B_doc_vectorized = doc_tfidf_vectorizer.transform([B_doc])

            elapsed_1, from_doc_similarity = cosine_pairwise_sim(A_doc_vectorized, B_doc_vectorized)

            # save to db
            ids = utils_db.get_bill_ids(billnumber_versions=[bill_A, bill_B])

            if len(ids) == 2 and ids[bill_A] != ids[bill_B]:
                print("Both bills found. Saving.")
                print("Doc similarity: ", from_doc_similarity[0][0])
                bill_id = ids[bill_A]
                bill_to_id = ids[bill_B]

                # TODO: should we save 2 bill to bill records for each comparison?
                bill_to_bill_new = pymodels.BillToBillModel(
                    billnumber_version= bill_A,
                    billnumber_version_to= bill_B,
                    score= from_doc_similarity[0][0],
                    bill_id= bill_id,
                    bill_to_id= bill_to_id
                )

                btb = utils_db.save_bill_to_bill(bill_to_bill_model= bill_to_bill_new)

with Flow("compare_es_related_bills", executor=LocalDaskExecutor()) as flow:
    bills_to_compare = get_bills_to_compare()
    calculate_bill_similarity(bills_to_compare)
    
flow.register(project_name="BillSimilarityEngine")