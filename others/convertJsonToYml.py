"""
Compare2Dataframe.py
~~~~~~~~
"""
import json
import yaml
import sys


def main():
    print("abc")
    f = open('/home/raag/Study/github/pyspark-example-project/comparing/file1.json')
    data = json.load(f)
    str = '{"Arrival_Time":1424686735175,"Creation_Time":1424686733176179000,"Device":"nexus4_1","Index":35,"Model":"nexus4","User":"g","gt":"stand","x":0.0014038086,"y":0.00050354,"z":-0.0124053955}'
    yaml_data = yaml.dump(data, default_flow_style=False)
    print(yaml_data)
    print(data)

#entry point for PySpark ETL application
if __name__ == '__main__':
    main()
