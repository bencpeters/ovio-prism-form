# Simple Flask wrapper to proxy form submissions into Air Table

import os
from functools import wraps
from urllib.parse import urlparse

# Load config variables from the environment variables, or .env file
from dotenv import load_dotenv
load_dotenv()

from airtable import Airtable
air_table = Airtable(os.getenv("AIRTABLE_BASE_KEY"), os.getenv("AIRTABLE_TABLE_NAME"))

from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
cors = CORS(app, resources={"/submit": {"origins": os.getenv("FORM_SERVER")}})

def transform_form_to_airtable(form_data):
    """Changes field names and remakes form data to match the schema expected by AirTable
    """
    def to_pos_int(field_name):
        def _to_pos_int(d):
            try:
                i = int(d[field_name])
                if i < 0:
                    raise ValueError("Must be a positive integer")
                return i
            except ValueError:
                return 0
        return _to_pos_int

    def lookup_or_process(f):
        try:
            return f(form_data)
        except TypeError:
            try:
                return form_data[f]
            except KeyError:
                return None

    field_map = {
        "Name": "name",
        "Email": "email",
        "Country & City of residence": lambda d: ", ".join([d["city"], d["country"]]),
        "LinkedIn": "linked-in",
        "GitHub": "github",
        "Have you ever volunteered for a nonprofit before? If yes,  please specify.": "non-profit-experience",
        "How many years of technical experience do you have?": to_pos_int("tech-exp"),
        "Are you a student?": "student",
        "Are you interested in mentoring?": "mentor",
        #"What role would you be most interested in playing?": ,
        #"What amazing skills are you bringing?": ,
        "Which causes are you most motivated by?": "causes",
        "How many hours can you commit weekly?": to_pos_int("hours"),
        "Are you interested in a specific project?": "project",
        "Is there anything else we should know about you?": "other",
        "Would you like to recommend other impactful projects or organizations that should be featured on the hub?": "recommendations",
        #"Other causes": ,
        "Company or University": "org",
        #"Other skills": ,
        #"Projects": ,
        #"Org. Proposed": ,
    }

    return {k: v for k, v in
            {k: lookup_or_process(v) for k, v in field_map.items()}.items()
            if v is not None}



def send_to_airtable(data):
    """Sends form data to airtable via API
    """
    air_table.insert(data)


def filter_by_request_url(f):
    """Crude filter for requests to attempt to ensure that they are coming from a valid host.
    Provides some modicum of a guard against automated pings to our API - not real security, but
    better than nothing"""
    @wraps(f)
    def decorated_func(*args, **kwargs):
        host = urlparse(request.environ['HTTP_ORIGIN'])
        allowed = urlparse(os.getenv("FORM_SERVER")) 
        if host.hostname != allowed.hostname or host.port != allowed.port:
            return jsonify({"Error": "Not authorized to make this request."}), 401
        else:
            return f(*args, **kwargs)
    return decorated_func


@app.route("/submit", methods=["POST"])
@filter_by_request_url
def submit():
    try:
        send_to_airtable(transform_form_to_airtable(request.form))
        return jsonify({"status": "success"})
    except Exception as err:
        app.logger.error(f"Error saving to AirTable: {err}")
        return jsonify({"Error": "Failed to save form data. Try resubmitting, or email support@oviohub.com if the probelm persists."}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0')