from __future__ import annotations

import os
import xmltodict
from snowflake.snowpark import Session
# from pprint import pprint


LOCAL_DRY_RUN = False
LOCAL_XML_PATH = 'data/dryrun/'
SF_XML_STAGE = '@<XML_LANDING>/processing/'


def unpack_xmls(session: Session) -> str:
    file_results = []
    err_cnt = 0

    for cloud_fname in get_xml_fnames_to_load(session):
        row_insert_cnt = 0
        file_msg = ''
        try:
            file_xml = read_xml_file(session, cloud_fname)
            file_data = xmltodict.parse(file_xml)
            xml_data = file_data['docList']
            # pprint(file_data)  # DBG
            ingest_times = check_if_file_ingested(session, xml_data['fileName'])
            if ingest_times == 0:
                row_insert_cnt = insert_in_tables(session, xml_data)
            else:
                file_msg = f'file was already ingested times={ingest_times}'
        except Exception as err:
            err_cnt += 1
            file_msg = str(err)
        finally:
            file_results.append((cloud_fname, row_insert_cnt, file_msg))

    results_msg = format_results(file_results)
    if err_cnt > 0:
        raise RuntimeError(results_msg)
    return results_msg


def get_xml_fnames_to_load(session: Session):
    if LOCAL_DRY_RUN:
        local_files = get_local_filenames(LOCAL_XML_PATH)
        print('processing: ', LOCAL_XML_PATH, local_files)
        return local_files

    files_data = exec_sql(session, f'LIST {SF_XML_STAGE}')

    def extract_fname(file_data):
        return file_data['name'].split('/')[-1]

    xml_files = [extract_fname(fd) for fd in files_data if extract_fname(fd).endswith('.xml')]
    return xml_files


def get_local_filenames(directory):
    try:
        filenames = os.listdir(directory)
        # Filter out directories, keeping only files
        return [f for f in filenames if os.path.isfile(os.path.join(directory, f))]
    except FileNotFoundError:
        return []


def check_if_file_ingested(session: Session, fname: str) -> int:
    command = '''
SELECT count(*) as count
FROM submission
WHERE filename = '{}';
    '''.format(fname)
    results = exec_sql(session, command)
    if LOCAL_DRY_RUN:
        return 0

    return int(results[0]['COUNT'])


def insert_in_tables(session: Session, data: dict) -> int:
    inserts = 0
    exec_sql(session, 'BEGIN TRANSACTION')
    try:
        inserts = dict_to_table(session, 'submission', data, data['fileName'])
        exec_sql(session, 'COMMIT')
    except Exception as err:
        exec_sql(session, 'ROLLBACK')
        raise err
    return inserts


def exec_sql(session: Session, command: str, params=None):
    if LOCAL_DRY_RUN:
        print(command, params)
        return None

    return session.sql(command, params).collect()


def read_xml_file(session: Session, fname: str):
    content = ''
    if LOCAL_DRY_RUN:
        with open(LOCAL_XML_PATH + fname, 'r', encoding='utf-8') as file:
            content = file.read()
    else:
        path = "'{}{}'".format(SF_XML_STAGE, fname)
        df = session.read.xml(path)
        dfp = df.to_pandas()
        content = dfp.iloc[0]["$1"]

    assert content, 'file is empty'
    return content


def dict_to_table(session: Session, table: str, data: dict, fname: str, fk=None, inserts=0):
    scalar_items = {}

    child_fk = build_foreign_key(table, data)
    for item in data.items():
        key, value = item

        if isinstance(value, dict):
            inserts = dict_to_table(session, key, value, fname, child_fk, inserts)

        elif isinstance(value, list):
            str_list = []
            for dataitem in value:
                if isinstance(dataitem, dict):
                    inserts = dict_to_table(session, key, dataitem, fname, child_fk, inserts)
                else:
                    if dataitem:
                        str_list.append(str(dataitem))
            if str_list:
                scalar_items[key] = ', '.join(str_list)

        else:
            if value:
                scalar_items[key] = value

    if len(scalar_items) > 0:
        scalar_items['fileName'] = fname
        insert_row(session, table, scalar_items, fk)
        inserts = inserts + 1
    return inserts


def build_foreign_key(parent_name: str, parent_data: dict) -> tuple[str, str] | None:
    fk = None
    if parent_name:
        fkid = parent_data.get('@id')
        if not fkid:
            fkid = parent_data.get('@refId')
        if not fkid:
            fkid = parent_data.get('refNumber')
        if fkid:
            fk = (f'fk_{parent_name}', fkid)
        else:
            print('!!! Error building FK: no id in tag',
                  parent_name, parent_data)
    return fk


def insert_row(session: Session, table: str, items: dict, fk=None):
    inserted = 0

    if fk:
        items[fk[0]] = fk[1]
    cols = list(items.keys())
    vals = list(items.values())
    sql_templ = build_insert_sql(table, cols, vals)
    try:
        exec_sql(session, sql_templ, params=[*vals])
        inserted += 1
    except Exception as err:
        raise RuntimeError(f'{err}: {sql_templ}')
    return inserted


def build_insert_sql(table: str, cols: list, vals: list) -> str:
    cols_clean = [c.replace('@', '') for c in cols]
    qmarks = ['?'] * len(cols_clean)
    cols_str = ', '.join(cols_clean)
    qmarks_str = ', '.join(qmarks)
    sql_templ = f'INSERT INTO {table}({cols_str}) VALUES({qmarks_str});'
    return sql_templ


def format_results(results) -> str:
    def fmt_msg(fres):
        return f'{fres[0]} -> {fres[1]} rows | {fres[2]}'

    messages = ['Resuls:']
    messages += [fmt_msg(r) for r in results]
    return ('\n').join(messages)


# For local debugging
# Beware you may need to type-convert arguments if you add input parameters
if __name__ == "__main__":
    # Create a local Snowpark session
    with Session.builder.config("local_testing", True).getOrCreate() as session:
        LOCAL_DRY_RUN = True
        result = unpack_xmls(session)
        print(result)
