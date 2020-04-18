#!/usr/bin/env python

__author__ = "Mageswaran Dhandapani"
__copyright__ = "Copyright 2020, The Spark Structured Playground Project"
__credits__ = []
__license__ = "Apache License"
__version__ = "2.0"
__maintainer__ = "Mageswaran Dhandapani"
__email__ = "mageswaran1989@gmail.com"
__status__ = "Education Purpose"

import os
import shutil
from flask import Flask, render_template, request, url_for, jsonify
import gin

from dataset_base import PostgresqlConnection
from pretty_print import print_error, print_info
from flask_paginate import Pagination, get_page_args

# https://gist.github.com/mozillazg/69fb40067ae6d80386e10e105e6803c9#file-index-html-L5
# https://github.com/doccano/doccano

PER_PAGE = 50

app = Flask(__name__)
app.debug = True

# STORE_PATH = os.path.expanduser("~") + "/ssp/text_tagger/"

@gin.configurable
class LabelsInfo(object):
    def __init__(self, labels=gin.REQUIRED):
        self.labels =labels


def check_n_mk_dirs(path, is_remove=False):
    if os.path.exists(path):
        if is_remove:
            shutil.rmtree(path)
    else:
        os.makedirs(path)


@app.route('/')
def index():
    """
    Home page with list of links for upload and download
    :return:
    """
    return render_template('layouts/index.html')


def get_subset(df, offset=0, per_page=PER_PAGE):
    return df.iloc[offset: offset + per_page]


@app.route('/tables_list', methods=['GET'])
def tables_list():
    try:
        db = PostgresqlConnection()
        tables_list = db.get_tables_list()
    except Exception as e:
        print_info(e)
        return jsonify("No files found!")

    # remove extension
    data_files = [table for table in tables_list
                  if table.startswith("test") or
                  table.startswith("dev") or  table.startswith("train")]
    return render_template('layouts/dumped_tables_list.html', len=len(data_files), files=data_files)


@app.route('/tag_table/<table_name>', methods=['GET', 'POST'])
def tag_table(table_name):
    """
    Creates paginated pages, displaying text and corresponding lables
    :return:
    """
    db = PostgresqlConnection()
    df = db.query_to_df(f"select count(*) as count from {table_name}")
    total = df["count"].values[0]

    # Label dataframe, store the dictinaries
    labels = LabelsInfo()
    string_2_index = labels.labels
    index_2_string = dict(zip(string_2_index.values(), string_2_index.keys()))

    if request.method == 'POST':
        """
        Form is used to capture the text id, label and other pagination info.
        When `submit` is clicked we will get it as a POST request
        """
        print_info("===========================POST==============================")
        # Parse the response
        response = request.form.to_dict()
        print(response)

        page, per_page, offset = int(response["page"]), int(response["per_page"]), int(response["offset"])

        for i in range(offset, offset+PER_PAGE):
            j = str(i+1) #index correction
            index = int(response["id"+j])
            label = string_2_index[response["option"+j]]
            db.run_query(f"UPDATE {table_name} SET label={label} WHERE text_id={index}")
    else:
        page, _, _ = get_page_args(page_parameter='page',
                                   per_page_parameter='per_page')
        offset = PER_PAGE * (page-1)
        print_error([page, PER_PAGE, offset])

    data_df = db.query_to_df(f"select * from {table_name} ORDER BY text_id limit {PER_PAGE} offset {offset}")

    print_info(data_df[["text_id", "text"]])

    # Pagination, listing only a subset at a time
    pagination = Pagination(page=page,
                            per_page=PER_PAGE,
                            total=total,
                            css_framework='bootstrap4')

    print_error(data_df["text_id"].to_list())
    # Naive way of sending all the information to the HTML page and get it back in POST command
    return render_template('layouts/db_table_tagger.html',
                           page=page,
                           per_page=PER_PAGE,
                           offset=offset,
                           pagination=pagination,
                           file=table_name,
                           url=url_for("tag_table", table_name=table_name),
                           len=data_df.shape[0],
                           id=data_df["text_id"].to_list(),
                           text=data_df["text"].to_list(),
                           label=data_df["label"].to_list(),
                           label_string=[index_2_string[int(i)] for i in data_df["label"].to_list()],
                           options=list(string_2_index.keys()))


@gin.configurable
def tagger(host,
           port):
    app.run(debug=True, host=host, port=port)


if __name__ == '__main__':
    gin.parse_config_file(config_file="tagger.gin")
    tagger()