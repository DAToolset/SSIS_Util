from lxml import etree
import os
import datetime
import csv
import argparse

# region XML Tag definition
pfx = '{www.microsoft.com/'
tag_Executable = pfx + 'SqlServer/Dts}Executable'
tag_ObjectName = pfx + 'SqlServer/Dts}ObjectName'
atr_refId = pfx + 'SqlServer/Dts}refId'
tag_Objectdata = pfx + 'SqlServer/Dts}ObjectData'
tag_SqlTaskData = pfx + 'sqlserver/dts/tasks/sqltask}SqlTaskData'
atr_ConnectionID = pfx + 'sqlserver/dts/tasks/sqltask}Connection'
atr_SqlStatementSource = pfx + 'sqlserver/dts/tasks/sqltask}SqlStatementSource'
atr_Disabled = pfx + 'SqlServer/Dts}Disabled'

# precedence
tag_PrecedenceConstraint = pfx + 'SqlServer/Dts}PrecedenceConstraint'
tag_From = pfx + 'SqlServer/Dts}From'
tag_To = pfx + 'SqlServer/Dts}To'

# connection
tag_ConnectionManager = pfx + 'SqlServer/Dts}ConnectionManager'
tag_CreationName = pfx + 'SqlServer/Dts}CreationName'
atr_ConnectionString = pfx + 'SqlServer/Dts}ConnectionString'
atr_DTSID = pfx + 'SqlServer/Dts}DTSID'

# variable
tag_Variable = pfx + 'SqlServer/Dts}Variable'
tag_Expression = pfx + 'SqlServer/Dts}Expression'
tag_Namespace = pfx + 'SqlServer/Dts}Namespace'
# endregion


def get_sql_using_xpath(file_name) -> (str, dict):

    tree = etree.parse(file_name)
    root = tree.getroot()

    con_dic = {}  # Key: ConnectionName , Value: ConnectionProperty
    con_id_dic = {}  # Key: ConnectionID , Value: ConnectionName
    con_cnt = 0
    var_dic = {}  # Key: "NameSpace::ObjectName", Value: Expression, etc...

    for ele in root.findall(f'.//{tag_ConnectionManager}[@{atr_DTSID}]'):  # extract connection list
        con_cnt += 1
        con_id = ele.attrib[atr_DTSID]
        con_name = ele.attrib[tag_ObjectName]
        cre_name = ele.attrib[tag_CreationName]
        con_str = ""
        con_ele = ele.find(f'.//{tag_Objectdata}/{tag_ConnectionManager}')
        if con_ele is not None and atr_ConnectionString in con_ele.attrib:
            con_str = con_ele.attrib[atr_ConnectionString]
        con_id_dic[con_id] = con_name
        con_dic[con_name] = f'{con_cnt}. {con_name}\n  - CreationName:{cre_name}\n  - ConnectionString:[{con_str}]'
    con_text = '\n\n'.join(f'{v}' for k, v in con_dic.items())

    for ele in root.findall(f'.//{tag_Variable}[@{tag_Expression}]'):  # extract variable list
        var_name = ele.attrib[tag_Namespace] + "::" + ele.attrib[tag_ObjectName]  # Key: "NameSpace::ObjectName"
        var_val = ele.attrib[tag_Expression]
        var_dic[var_name] = var_val

    sql_str = ""
    for ele in root.findall(f'.//{tag_Executable}/{tag_Objectdata}/{tag_SqlTaskData}/../..'):  # extract SQL list
        if atr_refId not in ele.attrib:
            continue
        ref_id = ele.attrib[atr_refId]
        sql_ele = ele.find(f'.//{tag_Objectdata}/{tag_SqlTaskData}[@{atr_SqlStatementSource}]')
        if is_disabled(ele):  # exclude Disabled task(also ancestor Elements)
            continue
        tsk_sql_str = ""
        tsk_con_name = ""
        tsk_con_str = ""
        if sql_ele is not None:
            tsk_con_id = sql_ele.attrib[atr_ConnectionID]
            tsk_con_name = con_id_dic[tsk_con_id]
            tsk_con_str = con_dic[tsk_con_name]
            tsk_sql_str = sql_ele.attrib[atr_SqlStatementSource]
            sql_str += f'/* [Control Flow(제어 흐름) TaskName: {ref_id}]\n   [Connection: {tsk_con_str}]\n*/\n{tsk_sql_str}'
            sql_str += get_line_separator()

    data_flow_str = ""
    for ele in root.findall(f'.//pipeline/components/component[@refId]'):  # extract data flow list
        if is_disabled(ele):  # exclude Disabled task(also ancestor Elements)
            continue
        ref_id = ele.attrib['refId']
        sql_cmd_var_str = ""; sql_cmd_str = ""; open_rowset_str = ""
        sql_cmd_var_ele = ele.find(f'.//property[@name="SqlCommandVariable"]')
        if sql_cmd_var_ele is not None:
            sql_cmd_var_str = sql_cmd_var_ele.text
        sql_cmd_ele = ele.find(f'.//property[@name="SqlCommand"]')
        if sql_cmd_ele is not None:
            sql_cmd_str = sql_cmd_ele.text
        open_rowset_ele = ele.find(f'.//property[@name="OpenRowset"]')
        if open_rowset_ele is not None and open_rowset_ele.text is not None:
            open_rowset_str = "OpenRowset: " + open_rowset_ele.text
        str_list = [sql_cmd_var_str, sql_cmd_str, open_rowset_str]
        try:
            data_flow_val = next(s for s in str_list if s)
        except:
            data_flow_val = ''

        con_str = ''
        con_ele = ele.find(f'.//connections/connection[@connectionManagerID]')
        if con_ele is not None:
            con_str = con_ele.attrib['connectionManagerID']
        data_flow_str += f'/* [Data Flow(데이터 흐름) TaskName: {ref_id}]\n   [Connection: {con_str}] */\n{data_flow_val}'
        data_flow_str += get_line_separator()

    prd_text = ""
    for ele in root.findall(f'.//{tag_PrecedenceConstraint}[@{tag_From}]'):  # make precedence string
        prd_from = ele.attrib[tag_From]
        prd_to = ele.attrib[tag_To]
        prd_text += f'[{prd_from}] --> [{prd_to}]\n'

    result_str = f"""/* Precedence Constraint(실행 순서)
{prd_text}*/
{get_line_separator()}
/* Connections(연결 정보)
{con_text}
*/
{get_line_separator()}
{data_flow_str}
{get_line_separator()}
{sql_str}"""

    return result_str, con_dic


def get_current_datetime() -> str:
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")


def get_line_separator() -> str:
    return f'\n\n/*{"*" * 100}*/\n\n'


def is_disabled(ele) -> bool:
    """check if the element has 'Disabled' attribute and it's value is 'True', and check parent element"""
    if atr_Disabled in ele.attrib and ele.attrib[atr_Disabled].lower() == "true":
        return True
    else:
        pele = ele.find('..')
        if pele is None:
            return False
        else:
            return is_disabled(pele)


def main():
    parser = argparse.ArgumentParser(formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('--in_path', required=True, type=str,
                        help='input path with dtsx files')
    parser.add_argument('--out_path', required=True, type=str,
                        help='output path with extracted sql files from dtsx files')
    args = parser.parse_args()
    in_path = os.path.abspath(args.in_path)
    out_path = os.path.abspath(args.out_path)
    file_list = []
    print(f'[{get_current_datetime()}] Start Get File List...')
    in_abspath = os.path.abspath(in_path)  # os.path.abspath('.') + '\\test_files'
    file_types = ('.dtsx',)
    for root, dir, files in os.walk(in_abspath):
        for file in sorted(files):
            # exclude
            if file.startswith('~'):
                continue
            # include
            if file.endswith(file_types):
                file_list.append(root + '\\' + file)

    print(f'[{get_current_datetime()}] Finish Get File List. ({len(file_list)} files)')

    print(f'[{get_current_datetime()}] Start Extract File Contents...')
    con_list = []
    for dtsx_file in file_list:
        print(dtsx_file)
        sql_file = dtsx_file.replace(in_path, out_path) + ".sql"
        sql_file_dir = os.path.dirname(sql_file)
        os.makedirs(name=sql_file_dir, exist_ok=True)
        result_str, con_dic = get_sql_using_xpath(dtsx_file)
        tmp_list = []
        for k, v in con_dic.items():
            tmp_list.append([dtsx_file, sql_file, k, v])
        if len(tmp_list) > 0:
            con_list.append(tmp_list)
        with open(sql_file, "w", encoding="utf8") as file:
            file.write(result_str)

    csv_file = out_path + "\\con_sql.csv"
    with open(csv_file, 'w', newline='', encoding='ansi') as file:
        writer = csv.writer(file)
        writer.writerow(["dtsx_file", "sql_file", "connection name", "connection property"])
        # writer.writerow(con_list)
        for con in con_list:
            for con2 in con:
                writer.writerow(con2)

    print(f'[{get_current_datetime()}] Finish Extract File Contents. ({len(file_list)} files)')


if __name__ == '__main__':
    main()

# sample dtsx file: https://github.com/LearningTechStuff/SSIS-Tutorial
