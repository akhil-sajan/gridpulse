"""Stand-ins for dbt_utils macros so sqlfluff's jinja templater can render the models
without a warehouse connection. They only need to produce parseable SQL."""


def generate_surrogate_key(field_list):
    return "md5(concat_ws('-', " + ", ".join(field_list) + "))"


def date_spine(datepart, start_date, end_date):
    return f"select explode(sequence({start_date}, {end_date})) as date_{datepart}"
