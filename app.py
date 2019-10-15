# Simple Flask wrapper to proxy form submissions into Air Table

import os
import sys
import logging
import re
from functools import wraps
from urllib.parse import urlparse
from requests.exceptions import HTTPError

# Load config variables from the environment variables, or .env file
from dotenv import load_dotenv
load_dotenv()

from airtable_helpers import transform_form_to_airtable, send_to_airtable

from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
cors = CORS(app, resources={"/submit": {"origins": os.getenv("FORM_SERVER")}})

# Heroku logging
if 'DYNO' in os.environ:
    app.logger.addHandler(logging.StreamHandler(sys.stdout))
    app.logger.setLevel(logging.ERROR)


def filter_by_request_url(f):
    """Crude filter for requests to attempt to ensure that they are coming from a valid host.
    Provides some modicum of a guard against automated pings to our API - not real security, but
    better than nothing"""
    @wraps(f)
    def decorated_func(*args, **kwargs):
        host = urlparse(request.environ['HTTP_ORIGIN'])
        form_server = os.getenv("FORM_SERVER")

        if re.match("^\[.+\]$", form_server) is not None:
            allowed = [item.strip() for item in form_server[1:-1].split(",")]
        else:
            allowed = [form_server]

        allowed = [urlparse(h) for h in allowed]
        if next((h for h in allowed if host.hostname == h.hostname and host.port == h.port), None) is None:
            return jsonify({"Error": "Not authorized to make this request."}), 401
        else:
            return f(*args, **kwargs)
    return decorated_func


@app.route("/submit", methods=["POST"])
@filter_by_request_url
def submit():
    try:
        data = transform_form_to_airtable(request.form)
        send_to_airtable(data)
    except HTTPError as err:
        app.logger.error(f"Error saving to AirTable: {err}\nAttempted Write: {data}")
        raise err

    return jsonify({"status": "success"})


@app.errorhandler(Exception)
def handle_error(e):
    app.logger.error(f"Error handling request: {e}")
    return jsonify({"Error": "Failed to save form data. Try resubmitting, or email support@oviohub.com if the problem persists."}), 500


if __name__ == '__main__':
    app.run(host='0.0.0.0')
