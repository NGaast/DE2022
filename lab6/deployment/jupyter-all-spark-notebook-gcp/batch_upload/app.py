from flask import Flask, jsonify, Response, request, render_template, flash, redirect

from resources.data_management import DataManagement
import requests
import sys
import os
import json
import secrets
import logging

app=Flask(__name__)

app.config['UPLOAD_FOLDER'] = "./tmp"

secret = secrets.token_urlsafe(32)
app.secret_key = secret


@app.route('/data_upload', methods=['GET', 'POST'])
def training_ui():
    return render_template("training_template.html")  # this method is called of HTTP method is GET, e.g., when browsing the link

@app.route('/upload_fixture_data', methods=['GET', 'POST'])
def upload_fixtures_data():
    data_manager = DataManagement('de-2022-ng', 'data_de2022_ng')
    if request.method == "POST":
        # No file in request
        if 'fixture_data' not in request.files:
            print("No file in request", file=sys.stdout)
            return redirect(request.url)
        # Retrieve file
        data_file = request.files['fixture_data']
        # No file
        if data_file.filename == '':
            print("No file selected", file=sys.stdout)
            sys.stdout.flush()
            return redirect(request.url)

        json_format = json.load(data_file)
        data_manager.store_json(json_format, "fixtures.json")

        print("Succesfully stored data", file=sys.stdout)
        sys.stdout.flush()

        return redirect('/data_upload')
    return redirect('/data_upload')

@app.route('/upload_statistics_data', methods=['GET', 'POST'])
def upload_statistics_data():
    data_manager = DataManagement('de-2022-ng', 'data_de2022_ng')
    if request.method == "POST":
        # No file in request
        if 'statistics_data' not in request.files:
            print("No file in request", file=sys.stdout)
            return redirect(request.url)
        # Retrieve file
        data_file = request.files['statistics_data']
        # No file
        if data_file.filename == '':
            print("No file selected", file=sys.stdout)
            sys.stdout.flush()
            return redirect(request.url)

        json_format = json.load(data_file)
        data_manager.store_json(json_format, "statistics.json")

        print("Succesfully stored data", file=sys.stdout)
        sys.stdout.flush()

        return redirect('/data_upload')
    return redirect('/data_upload')
    

app.run(host='0.0.0.0', port=5000)
