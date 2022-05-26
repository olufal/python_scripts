"""
Parses XML in a CSV file with Recursion
"""

from typing import Dict, List

import xmltodict
import pandas as pd


def get_values(qg: Dict,
               results: List,
               question_group_id,
               path_name,
               sort_num) -> List:
    if 'QuestionGroup' in qg.keys():
        if isinstance(qg['QuestionGroup'], dict):
            question_group_id = qg['@ID'] if question_group_id == 0 else question_group_id
            path_desc = qg['@Name'] if path_name == '' else f"{path_name}, {qg['@Name']}"
            sort_desc = f'{sort_num}'
            data = {'QuestionGroupID': question_group_id,
                    'PathDesc': f'{path_desc}',
                    'SortDesc': f'{sort_desc}'}
            results.append(data)
            # print(f"Appended to result list: {question_group_id} - {path_desc}")

        if isinstance(qg['QuestionGroup'], list):
            for i, val in enumerate(qg['QuestionGroup']):
                qg_id = val['@ID']
                path_desc = f"{val['@Name']}" if path_name == '' else f"{path_name}, {val['@Name']}"
                sort_desc = f'{sort_num}{(i + 1):02}'
                data = {'QuestionGroupID': qg_id,
                        'PathDesc': f'{path_desc}',
                        'SortDesc': f'{sort_desc}'}
                results.append(data)
                # print(f"Appended to result list: {question_group_id} - {path_desc}")
                get_values(val, results, qg_id, path_desc, sort_desc)
    return results


def parse_xml(xml: str) -> pd.DataFrame:
    """
    Parses a xml
    """
    disease_dict = xmltodict.parse(xml)['Disease']

    result = get_values(disease_dict, results=[], question_group_id=0, path_name='', sort_num='Sort_')
    return pd.DataFrame(result)


def unit_test():
    # unit test
    df = pd.read_csv('DiseaseVersion.csv')
    test_xml = df['DiseaseVersionXML'][3]
    test_dict = xmltodict.parse(test_xml)['Disease']
    test_results = get_values(test_dict, results=[], question_group_id=0, path_name='', sort_num='Sort_')
    test_df = pd.DataFrame(test_results)
    return test_df


def main():
    df = pd.read_csv('DiseaseVersion.csv')
    results = pd.concat([pd.concat([parse_xml(r['DiseaseVersionXML']), pd.DataFrame(r).T], axis=1)
                        .bfill()
                        .ffill()
                         for _, r in df.iterrows()], ignore_index=True)
    return results
