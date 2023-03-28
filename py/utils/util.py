
import jsonpickle
import os

class LineChatSetting:
    def __init__(self) -> None:
        self.linewidth = 1
        self.markersize = 10
        
def from_json(json_text):
    return jsonpickle.decode(json_text)

def json_file(file):
    json_path = os.path.abspath(__file__ + "/../../json")

    path = os.path.join(json_path, file)

    return path

def json_values(file, filter=None):
    result = []
    f = open(json_file(file))
    for line in f.readlines():
        if line.isspace():
            continue
        r = from_json(line)
        if filter is None or filter(r):
            result.append(r)
    return result

def json_attr_values(file, attr_names=[], filter=None):
    result = {}
    f = open(json_file(file))
    for line in f.readlines():
        if line.isspace():
            continue
        r = from_json(line)
        if filter is None or filter(r):
            for attr in attr_names:
                value = r[attr]
                if attr in result.keys():
                    result[attr].append(value)
                else:
                    result[attr] = [value]
    return result



