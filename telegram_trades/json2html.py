import json


def remove_dict_of_dict(dct):
    filtered_dct = {key: value for key, value in dct.items()
                    if not isinstance(value, dict)}
    return filtered_dct


def dcts_of_dct(dct):
    dct_only = {key: value for key, value in dct.items()
                if isinstance(value, dict)}
    return dct_only


def dict_to_html(dictionary):
    html_content = "<table border='1'>"
    headers = dictionary.keys()
    for header in headers:
        html_content += "<th>{}</th>".format(header)
    html_content += "</tr>"

    html_content += "<tr>"
    values = dictionary.values()
    for value in values:
        html_content += "<td>{}</td>".format(value)
    html_content += "</tr>"
    html_content += "</table>"
    return html_content


def lst_of_dcts_to_html(json_data):
    html_content = "<table border='1'><tr>"
    # Extracting column headers
    for dct in json_data:
        first_child = list(dct.values())[0]
        break
    headers = first_child.keys()
    for header in headers:
        html_content += "<th>{}</th>".format(header)
    html_content += "</tr>"

    # Extracting data rows
    for dictionary in json_data:
        dictionaries = dictionary.values()
        for dct in dictionaries:
            values = dct.values()
            html_content += "<tr>"
            for value in values:
                html_content += "<td>{}</td>".format(value)
            html_content += "</tr>"
    html_content += "</table>"
    return html_content


if __name__ == "__main__":
    from constants import DATA, F_TASK

    # Load JSON data from file
    with open(F_TASK, 'r') as file:
        json_data = json.load(file)
    html_content = ""
    for dct in json_data:
        filtered_dct = {key: value for key, value in dct.items()
                        if not isinstance(value, dict)}
        html_content += dict_to_html(filtered_dct)
        dcts_only = {key: value for key, value in dct.items()
                     if isinstance(value, dict)}
        if any(dcts_only):
            html_content += lst_of_dcts_to_html([dcts_only])

# Write HTML content to a file
    with open(DATA + 'task.html', 'w') as file:
        file.write(html_content)
