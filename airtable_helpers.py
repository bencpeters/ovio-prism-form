# Helper functions for transforming data & querying AirTable

import os

from airtable import Airtable
volunteer_table = Airtable(
    os.getenv("AIRTABLE_BASE_KEY"), os.getenv("AIRTABLE_TABLE_NAME"))


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
        res = f(form_data)
    except TypeError:
        try:
            res = form_data[f]
        except KeyError:
            return None

    # If we've got a list, filter out None values. Need an isinstance check since strings act like
    # lists
    if not isinstance(res, str):
        try:
            res = _filter_none(res)
        except TypeError:
            pass

    return res


def _list_to_string(values):
    """Helper to transform a list of values into a comma separated string
    """
    return ", ".join(values)


def _convert_to_record_ids(names, linked_table):
    """Takes a list of names and converts them to record ids based on the `linked_table` provided
    """
    return [linked_table[n] for n in names]


def _nonrelational_list_inputs(names, linked_table):
    """Helper to get the list of values that don't correspond to records in the `linked_table`
    """
    return [n for n in names if linked_table[n] is None]


def _filter_in_list(values, members):
    """Helper to get the portion of a list that is contained in `members`
    """
    return [v for v in values if v in members]


def _filter_not_in_list(values, members):
    """Helper to get the portion of a list that is not contained in `members`
    """
    return [v for v in values if v not in members]


def _remap_list(values, mapping):
    """Helper gets a list of values and remaps them based on the given map
    """
    return [mapping[v] if v in mapping else v for v in values]


def _filter_none(values):
    """Helper to filter out None values in a list
    """
    return [v for v in values if v is not None]


VALID_ROLES = [
    'Developer',
    'Project Lead',
    'System Architect',
    'QA',
    'Designer',
]

VALID_TYPES = [
    'Mentoring Organizations',
    'Mentoring Students',
    'Volunteering - Project Based',
    'Volunteering - Task Based',
]

CAUSES_MAP = {
    "No Poverty": "1. No Poverty",
    "Zero Hunger": "2. Zero Hunger",
    "Good Health & Well-Being": "3. Good Health and Well-being",
    "Quality Education": "4. Quality Education",
    "Gender Equality": "5. Gender Equality",
    "Clean Water & Sanitation": "6. Clean Water and Sanitation",
    "Affordable & Clean Energy": "7. Affordable and Clean Energy",
    "Decent Work & Economic Growth": "8. Decent Work and Economic Growth",
    "Industry Innovation & Infrastructure": "9. Industry Innovation and Infrastructure",
    "Reducing Inequality": "10. Reduced Inequalities",
    "Sustainable Cities & Communities": "11. Sustainable Cities and Communities",
    "Responsible Consumption & Production": "12. Responsible Consumption and Production",
    "Climate Action": "13. Climate Action",
    "Life below Water": "14. Life below Water",
    "Life on Land": "15. Life on Land",
    "Peace, Justice, & Strong Institutions": "16: Peace, Justice and Strong Institutions",
    "Partnerships to Achieve the Goal": "17: Partnerships to achieve the Goal",
}


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
    "What role would you be most interested in playing?":
        lambda d: _filter_in_list(_get_list("roles[]")(d), VALID_ROLES),
    "volunteering_type":
        lambda d: _filter_in_list(_get_list("types[]")(d), VALID_TYPES),
    "other_roles":
        lambda d: _list_to_string(_filter_not_in_list(
            _get_list("roles[]")(d), VALID_ROLES)),
    "What amazing skills are you bringing?":
        lambda d: _convert_to_record_ids(
            _get_list("skills[]")(d), skills_table),
    "Which causes are you most motivated by?":
        lambda d: _convert_to_record_ids(_remap_list(
            _get_list("causes[]")(d), CAUSES_MAP), causes_table),
    "How many hours can you commit weekly?": _to_pos_int("hours"),
    "Are you interested in a specific project?": "project",
    "Is there anything else we should know about you?": "other",
    "Would you like to recommend other impactful projects or organizations that should be featured on the hub?": "recommendations",
    "Other causes ?":
        lambda d: _list_to_string(_nonrelational_list_inputs(
            _remap_list(_get_list("causes[]")(d), CAUSES_MAP), causes_table)),
    "Company or University": "org",
    "Other skills?":
        lambda d: _list_to_string(_nonrelational_list_inputs(
            _get_list("skills[]")(d), skills_table)),
    # "Projects": ,
    # "Org. Proposed": ,
}


def transform_form_to_airtable(form_data):
    """Changes field names and remakes form data to match the schema expected by AirTable
    """
    return {k: v for k, v in
            {k: _lookup_or_call(form_data, v)
             for k, v in FIELD_MAP.items()}.items()
            if v is not None}


def send_to_airtable(data):
    """Sends form data to airtable via API
    """
    volunteer_table.insert(data)
