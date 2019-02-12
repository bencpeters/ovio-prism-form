# Helper functions for transforming data & querying AirTable

import os

from airtable import Airtable
volunteer_table = Airtable(os.getenv("AIRTABLE_BASE_KEY"), os.getenv("AIRTABLE_TABLE_NAME"))

class AirTableLinkedRecords(object):
    """Class to look up linked records in the specified AirTable table.
    """
    def __init__(self, table_name, key_col="Name", use_cache=True):
        self.name = table_name
        self.key_col = key_col
        self._cache = {}
        self._table = None
        self._use_cache = use_cache

    def __getitem__(self, record_name):
        try:
            return self._cache[record_name]
        except KeyError:
            record_id = self.record_id_by_name(record_name)
            if self._use_cache is True:
                self._cache[record_name] = record_id
            return record_id

    @property
    def table(self):
        """Lazily load the AirTable object"""
        if self._table is None:
            self._table = Airtable(os.getenv("AIRTABLE_BASE_KEY"), self.name)
        return self._table

    def record_id_by_name(self, record_name, key_col=None):
        """Finds records where `key_col == record_name`. For now we are lazy and just use the first
        such record found, assuming it to be unique...
        """
        if key_col is None:
            key_col = self.key_col

        res = self.table.search(key_col, record_name)
        if len(res) == 0:
            return None
        else:
            return res[0]["id"]

skills_table = AirTableLinkedRecords("Skills")
causes_table = AirTableLinkedRecords("Causes")

def _to_pos_int(field_name):
    """Helper creates an accessor function to search for the specified `field_name` and convert it
    to a positive integer

    Returns a calleable 
    """
    def _to_pos_int_inner(d):
        try:
            i = int(d[field_name])
            if i < 0:
                raise ValueError("Must be a positive integer")
            return i
        except ValueError:
            return 0
    return _to_pos_int_inner


def _checkbox_to_bool(field_name):
    """Helper to create an accessor function for coercing a boolean value from the `field_name`

    Returns a calleable
    """
    def _checkbox_to_bool_inner(d):
        if field_name in d and d[field_name] == "on":
            return True
    return _checkbox_to_bool_inner


def _get_list(field_name):
    """Helper to create an accessor function for converting the specified field into a list. This
    assumes that the passed in Dict is a Flask `request.form` and `getlist` will operate on field 
    names in the "xxxx[]" format as usual.

    Returns a calleable
    """
    def _get_list_inner(d):
        return d.getlist(field_name)
    return _get_list_inner


def _lookup_or_call(form_data, f):
    """Helper to either call `f` on `form_data`, or look up key `f` in `form_data`, depending on
    the type of `f`
    """
    try:
        return f(form_data)
    except TypeError:
        try:
            return form_data[f]
        except KeyError:
            return None

def _convert_to_record_ids(field_name, linked_table):
    def _convert_to_record_ids_inner(d):
        records = [l for l in [linked_table[l] for l in _get_list(field_name)(d)]
                   if l is not None]
        return records
    return _convert_to_record_ids_inner


def _nonrelational_list_inputs(field_name, linked_table):
    """Helper to get the list of values that don't correspond to records in the `linked_table`
    """
    def _nonrelational_list_inputs_inner(d):
        records = [l for l in _get_list(field_name)(d) if linked_table[l] is None]
        return ", ".join(records)
    return _nonrelational_list_inputs_inner


FIELD_MAP = {
    "Name": "name",
    "Email": "email",
    "Country & City of residence": lambda d: ", ".join([d["city"], d["country"]]),
    "LinkedIn": "linked-in",
    "GitHub": "github",
    "Have you ever volunteered for a nonprofit before? If yes,  please specify.": "non-profit-experience",
    "How many years of technical experience do you have?": _to_pos_int("tech-exp"),
    "Are you a student?": _checkbox_to_bool("student"),
    "Are you interested in mentoring?": _checkbox_to_bool("mentor"),
    "What role would you be most interested in playing?": _get_list("roles[]"),
    "What amazing skills are you bringing?": _convert_to_record_ids("skills[]", skills_table),
    "Which causes are you most motivated by?": _convert_to_record_ids("causes[]", causes_table),
    "How many hours can you commit weekly?": _to_pos_int("hours"),
    "Are you interested in a specific project?": "project",
    "Is there anything else we should know about you?": "other",
    "Would you like to recommend other impactful projects or organizations that should be featured on the hub?": "recommendations",
    "Other causes ?": _nonrelational_list_inputs("causes[]", causes_table),
    "Company or University": "org",
    "Other skills?": _nonrelational_list_inputs("skills[]", skills_table),
    #"Projects": ,
    #"Org. Proposed": ,
}


def transform_form_to_airtable(form_data):
    """Changes field names and remakes form data to match the schema expected by AirTable
    """
    return {k: v for k, v in
            {k: _lookup_or_call(form_data, v) for k, v in FIELD_MAP.items()}.items()
            if v is not None}


def send_to_airtable(data):
    """Sends form data to airtable via API
    """
    volunteer_table.insert(data)