import streamlit as st
import hydralit_components as hc
import datetime
import pandas as pd
from PIL import Image
from streamlit_option_menu import option_menu
import numpy as np
from dateutil import parser
import openpyxl
from datetime import datetime
from datetime import date
import time
import re
from dateutil.relativedelta import relativedelta 

#image_green_bar = Image.open(
#    'C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/Green_bar.jpg')
#image_ccb_logo = Image.open(
#    'C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/CCB_logo.jpg')

#st.image(image_green_bar)
#st.image(image_ccb_logo)


def get_data_profiling_stats(df, batch_start_date):
    df.columns = df.columns.str.strip()
    no_of_rows = len(df.columns)

    data_qlt_df = pd.DataFrame(index=np.arange(0, no_of_rows),
                               columns=('column_name', 'col_data_type', 'non_null_values',
                                        'unique_values_count', 'column_dtype')
                               )

    # Add rows to the data_qlt_df dataframe
    for ind, cols in enumerate(df.columns):
        # Count of unique values in the column
        col_unique_count = df[cols].nunique()

        data_qlt_df.loc[ind] = [cols,
                                df[cols].dtype,
                                df[cols].count(),
                                col_unique_count,
                                cols + '~' + str(df[cols].dtype)
                                ]

    # Use describe() to get column stats of raw dataframe
    # This will be merged with the DPD
    raw_num_df = df.describe().T.round(2)

    # ----- Key Step ---------------
    # Merging the df.describe() output with rest of the info to create a single Data Profile Dataframe
    data_qlt_df = pd.merge(data_qlt_df, raw_num_df, how='left', left_on='column_name', right_index=True)

    # Calculate percentage of non-null values over total number of values
    data_qlt_df['%_of_non_nulls'] = (data_qlt_df['non_null_values'] / df.shape[0]) * 100

    # Calculate null values for the column
    data_qlt_df['null_values'] = df.shape[0] - data_qlt_df['non_null_values']

    # Calculate percentage of null values over total number of values
    data_qlt_df['%_of_nulls'] = 100 - data_qlt_df['%_of_non_nulls']

    # Calculate the count of each data type
    data_qlt_df["dtype_count"] = data_qlt_df.groupby('col_data_type')["col_data_type"].transform('count')

    # Calculate the total count of column values
    data_qlt_df["Total_count"] = data_qlt_df['null_values'] + data_qlt_df['non_null_values']

    data_qlt_df['File Name'] = data_file
    data_qlt_df['Data Domain'] = data_domain
    data_qlt_df['Data Partner'] = data_entity
    data_qlt_df['Batch ID'] = batch_id
    data_qlt_df['Job ID'] = job_id
    data_qlt_df['Batch Start Date'] = batch_start_date
    data_qlt_df['Batch End Date'] = datetime.now()
    # Reorder the Data Profile Dataframe columns
    data_qlt_df = data_qlt_df[
        ['Batch ID', 'Job ID', 'Data Domain', 'Data Partner', 'File Name', 'column_name', 'col_data_type',
         'Total_count', 'non_null_values', '%_of_non_nulls',
         '%_of_nulls', 'unique_values_count', 'mean', 'std', 'min', 'max', 'Batch Start Date', 'Batch End Date']]

    return data_qlt_df


def data_length_check(df, col, rule_value):
    total_count_without_null = df[col].count()
    total_count_with_null = df[col].isnull().sum().sum()
    total_count = total_count_without_null + total_count_with_null
    df_remove_null = df[col].dropna()
    value_list = df_remove_null.values.tolist()
    defined_length = rule_value
    counter = 0
    col_length = len(col)
    for i in value_list:
        # print(i)
        if len(str(i)) == defined_length:
            counter = counter + 1
    result = counter / total_count * 100
    return counter, (total_count - counter), total_count, result


def date_range_check(df, col):
    passed = 0
    failed = 0
    total = 0
    result = 0
    return passed, failed, total, result


def date_pattren_check(format):
    if format == "MM/DD/YYYY":
        date_pattern = "%m/%d/%Y"
    elif format == "DD/MM/YYYY":
        date_pattern = "%d/%m/%Y"
    elif format == "YYYY/DD/MM":
        date_pattern = "%Y/%d/%m"
    elif format == "YYYY/MM/DD":
        date_pattern = "%Y/%m/%d"
    elif format == "MM-DD-YYYY":
        date_pattern = "%m-%d-%Y"
    elif format == "YYYY-MM-DD":
        date_pattern = "%Y-%m-%d"
    elif format == "YYYY-DD-MM":
        date_pattern = "%Y-%d-%m"
    elif format == "DD-MM-YYYY":
        date_pattern = "%d-%m-%Y"
    elif format == "DDMONYYYY:HH:MI:SS":
        date_pattern = "%d%b%Y:%H:%M:%S"
    return date_pattern


def date_format_check(df, col, rule_logic):
    counter = 0
    df_remove_null = df[col].dropna()
    value_list = df_remove_null.values.tolist()
    date_pattern = date_pattren_check(rule_logic)

    for i in value_list:
        try:
            bool(datetime.strptime(i, date_pattern))
        except:
            counter = counter + 1
        # print(parser.parse(i))
    total_count_without_null = df[col].count()
    total_count_with_null = df[col].isnull().sum().sum()
    total_count = total_count_without_null + total_count_with_null
    result = 100 - (((counter + total_count_with_null) / total_count) * 100)
    return (total_count - (counter + total_count_with_null)), (counter + total_count_with_null), total_count, result


# def specific_value_check(col):
def specific_value_check(df, col, data_type_format, rule_value, rule_logic):
    total_count_without_null = df[col].count()
    total_count_with_null = df[col].isnull().sum().sum()
    total_count = total_count_without_null + total_count_with_null
    df_remove_null = df[col].dropna()
    value_list = df_remove_null.values.tolist()
    counter = 0
    if data_type_format == 'Int':
        if rule_value[0] == '<':
            if rule_value[1] == '=':
                for i in value_list:
                    if int(i) <= int(rule_value[2:]):
                        counter = counter + 1
            else:
                for i in value_list:
                    if int(i) < int(rule_value[1:]):
                        counter = counter + 1
        elif rule_value[0] == '>':
            if rule_value[1] == '=':
                for i in value_list:
                    if int(i) >= int(rule_value[2:]):
                        counter = counter + 1
            else:
                for i in value_list:
                    if int(i) > int(rule_value[1:]):
                        counter = counter + 1
    elif data_type_format == 'Age':
        date_pattern = date_pattren_check(rule_logic)
        if rule_value[0] == '<':
            if len(rule_value) != 1:
                if rule_value[1] == '=':
                    for i in value_list:
                        j = get_age_in_years(i, date_pattern)
                        if int(j) <= int(rule_value[2:]):
                            counter = counter + 1
                else:
                    for i in value_list:
                        j = get_age_in_years(i, date_pattern)
                        if int(j) < int(rule_value[1:]):
                            counter = counter + 1
            else:
                for i in value_list:
                    j = get_age_in_years(i, date_pattern)
                    if int(j) < int(rule_value[1:]):
                        counter = counter + 1
        elif rule_value[0] == '>':
                if len(rule_value) != 1:
                    if rule_value[1] == '=':
                        for i in value_list:
                            j = get_age_in_years(i, date_pattern)
                            if int(j) >= int(rule_value[2:]):
                                counter = counter + 1
                    else:
                        for i in value_list:
                            j = get_age_in_years(i, date_pattern)
                            if int(j) > int(rule_value[1:]):
                                counter = counter + 1
                else:
                    for i in value_list:
                        j = get_age_in_years(i, date_pattern)
                        if int(j) > int(rule_value[1:]):
                            counter = counter + 1
        elif rule_value[0] == '=':
            for i in value_list:
                j = get_age_in_years(i, date_pattern)
                if int(j) >= int(rule_value[1:]):
                    counter = counter + 1
    elif data_type_format == 'Date':
        date_pattern = date_pattren_check(rule_logic)
        if rule_value[0] == '<':
            if len(rule_value) != 1:
                if rule_value[1] == '=':
                    for i in value_list:
                        df_date = get_age_in_days(i, date_pattern)
                        k = rule_value[2:]
                        if k == "":
                            today_date = datetime.date.today()
                        else:
                            df_col = df[k]
                            today_date = get_age_in_days(df_col, date_pattern)
                        if df_date <= today_date:
                            counter = counter + 1
                else:
                    for i in value_list:
                        df_date = get_age_in_days(i, date_pattern)
                        k = rule_value[1:]
                        if k == "":
                            today_date = datetime.date.today()
                        else:
                            df_col = df[k]
                            today_date = get_age_in_days(df_col, date_pattern)
                        if df_date < today_date:
                            counter = counter + 1
            else:
                for i in value_list:
                    df_date = get_age_in_days(i, date_pattern)
                    k = rule_value[1:]
                    if k == "":
                        today_date = date.today()
                    else:
                        df_col = df[k]
                        today_date = get_age_in_days(df_col, date_pattern)
                    if df_date < today_date:
                        counter = counter + 1
        elif rule_value[0] == '>':
            if len(rule_value) != 1:
                    if rule_value[1] == '=':
                        for i in value_list:
                            df_date = get_age_in_days(i, date_pattern)
                            k = rule_value[2:]
                            if k == "":
                                today_date = datetime.date.today()
                            else:
                                df_col = df[k]
                                today_date = get_age_in_days(df_col, date_pattern)
                            if df_date >= today_date:
                                counter = counter + 1
                    else:
                        for i in value_list:
                            df_date = get_age_in_days(i, date_pattern)
                            k = rule_value[1:]
                            if k == "":
                                today_date = datetime.date.today()
                            else:
                                df_col = df[k]
                                today_date = get_age_in_days(df_col, date_pattern)
                            if df_date > today_date:
                                counter = counter + 1
        elif rule_value[0] == '=':
            for i in value_list:
                df_date = get_age_in_days(i, date_pattern)
                k = rule_value[2:]
                if k == "":
                    today_date = datetime.date.today()
                else:
                    df_col = df[k]
                    today_date = get_age_in_days(df_col, date_pattern)
                if df_date >= today_date:
                    counter = counter + 1
    elif data_type_format == 'Varchar':
        for i in value_list:
            if str(i).lower() == rule_value.lower():
                counter = counter + 1

    result = (counter / total_count) * 100
    return counter, (total_count - counter), total_count, result


def get_age_in_years(value, date_pattern):
    import datetime
    from dateutil.relativedelta import relativedelta
    from dateutil.parser import parse
    #parsed_date = str(parse(value))
    # date_pattern = "%Y-%m-%d %H:%M:%S"
    birth_date = datetime.datetime.strptime(value, date_pattern).date()
    age = relativedelta(datetime.date.today(), birth_date).years
    return age


def get_age_in_days(value, date_pattern):
    import datetime
    from dateutil.relativedelta import relativedelta
    from dateutil.parser import parse
    #parsed_date = str(parse(value))

    # date_pattern = "%Y-%m-%d %H:%M:%S"
    df_date = datetime.datetime.strptime(value, date_pattern).date()
    #age = relativedelta(datetime.date.today(), birth_date).days
    return df_date


# def list_value_check(col):
def list_of_value_code_check(df, col, rule_value):
    total_count_without_null = df[col].count()
    total_count_with_null = df[col].isnull().sum().sum()
    total_count = total_count_without_null + total_count_with_null
    df_remove_null = df[col].dropna()
    value_list = df_remove_null.values.tolist()
    rule_value = list(rule_value.split(","))
    counter = 0
    for i in value_list:
        # print(i)
        if i in rule_value:
            counter = counter + 1
    result = (counter / len(value_list)) * 100
    return counter, (total_count - counter), total_count, result


def null_value_check(df, col):
    counter = df[col].isnull().sum()
    total_count = df[col].count()
    result = (counter / total_count) * 100
    return (total_count - counter), counter, total_count, result


def blank_value_check(df, col):
    counter = df[col].isnull().sum()
    total_count = df[col].count()
    result = (counter / total_count) * 100
    return (total_count - counter), counter, total_count, result


def numeric_check(df, col):
    df['col_is_digit'] = list(map(lambda x: str(x).isdigit(), df[col]))
    total_count = df[col].count()
    counter = df[df["col_is_digit"] == "False"]["col_is_digit"].count()
    result = (counter / total_count) * 100

    return (total_count - counter), counter, total_count, result


def referential_integrity_check(df, col):
    passed = 0
    failed = 0
    total = 0
    result = 0
    return passed, failed, total, result


def duplicate_check(df, col):
    total_count_without_null = df[col].count()
    total_count_with_null = df[col].isnull().sum().sum()
    total_count = total_count_without_null + total_count_with_null
    distinct_count = df[col].nunique()
    result = ((total_count - distinct_count) / total_count) * 100
    return distinct_count, (total_count - distinct_count), total_count, result


def pattern_check(df, col, rule_value):
    total_count_without_null = df[col].count()
    total_count_with_null = df[col].isnull().sum().sum()
    total_count = total_count_without_null + total_count_with_null
    value_list = df[col].values.tolist()
    df_remove_null = df[col].dropna()
    value_list = df_remove_null.values.tolist()
    counter = 0
    for i in value_list:
        if rule_value in i:
            counter = counter + 1
    result = counter / total_count * 100
    return counter, (total_count - counter), total_count, result


def business_check(df, col):
    passed = 0
    failed = 0
    total = 0
    result = 0
    return passed, failed, total, result


def write_excel_file(df, file_name, sheet_name):
    try:
        df_excel = pd.read_excel(file_name, sheet_name=sheet_name)
        result = pd.concat([df_excel, df], ignore_index=True)
        result.to_excel(file_name, sheet_name=sheet_name, index=False)
    except:
        print("Error: Not able to write the file")
    return


def read_csv_file(filename, delimiter):
    try:
        df1 = pd.read_csv(filename, delimiter=delimiter)
    except:
        print("Error: Data file not found!")
    return df1


def read_excel_file(filename, sheet):
    df = pd.read_excel(filename, sheet_name=sheet)
    return df


def calculate_score(df_file_name, df_attributes, df_file, df_dq_weightage):
    df_dq_weightage = df_dq_weightage[df_dq_weightage["Table_File ID"].isin(data_table_file_id)].fillna(0)
    source_accuracy = df_dq_weightage["DQ- Accuracy Weightage (%)"].values.tolist()[0]
    source_conformity = df_dq_weightage["DQ- Conformity Weightage (%)"].values.tolist()[0]
    source_completeness = df_dq_weightage["DQ- Completeness Weightage (%)"].values.tolist()[0]
    source_validity = df_dq_weightage["DQ- Validity Weightage (%)"].values.tolist()[0]
    source_timeliness = df_dq_weightage["DQ- Timeliness Weightage (%)"].values.tolist()[0]
    source_integrity_consistency = df_dq_weightage["DQ- Integrity & Consistency Weightage (%)"].values.tolist()[0]
    source_uniqueness = df_dq_weightage["DQ- Uniqueness Weightage (%)"].values.tolist()[0]

    conformity = 0
    total_conformity = 0
    accuracy = 0
    total_accuracy = 0
    completeness = 0
    total_completeness = 0
    validity = 0
    total_validity = 0
    uniqueness = 0
    total_uniqueness = 0
    int_consis = 0
    total_int_consis = 0

    for index, row in df_attributes.iterrows():
        data_attribute = row["Data Attribute"]
        rule_name = row["Rule Name"]
        rule_value = row["Rule Value"]
        data_type_format = row["Data type Format"]
        rule_logic = row["Rule Logic"]
        if rule_name == "Blank Value Check":
            passed, failed, total, result = blank_value_check(df_file, data_attribute)
            pass_percentage = (passed / total) * 100
            weighted_percentage = (pass_percentage * source_completeness) / 100
            completeness = completeness + weighted_percentage
            total_completeness = total_completeness + 1
            df = create_col_exec_details(batch_id, job_id, data_domain, data_entity, df_file_name, data_attribute,
                                         rule_name, total, passed, failed, pass_percentage, "Completeness",
                                         source_completeness, weighted_percentage, batch_start_date)
            write_excel_file(df,
                             "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/DQ_Column_Exec_Results.xlsx",
                             "DQ Column Exec RESULTS")

        elif rule_name == "Duplicate Check":
            passed, failed, total, result = duplicate_check(df_file, data_attribute)
            pass_percentage = (passed / total) * 100
            weighted_percentage = (pass_percentage * source_uniqueness) / 100
            uniqueness = uniqueness + weighted_percentage
            total_uniqueness = total_uniqueness + 1
            df = create_col_exec_details(batch_id, job_id, data_domain, data_entity, df_file_name, data_attribute,
                                         rule_name, total, passed, failed, pass_percentage, "Uniqueness",
                                         source_uniqueness, weighted_percentage, batch_start_date)
            write_excel_file(df,
                             "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/DQ_Column_Exec_Results.xlsx",
                             "DQ Column Exec RESULTS")
        elif rule_name == "Specific Value Check":
            passed, failed, total, result = specific_value_check(df_file, data_attribute, data_type_format, rule_value,
                                                                 rule_logic)
            pass_percentage = (passed / total) * 100
            weighted_percentage = (pass_percentage * source_accuracy) / 100
            accuracy = accuracy + weighted_percentage
            total_accuracy = total_accuracy + 1
            df = create_col_exec_details(batch_id, job_id, data_domain, data_entity, df_file_name, data_attribute,
                                         rule_name, total, passed, failed, pass_percentage, "Accuracy",
                                         source_accuracy, weighted_percentage, batch_start_date)
            write_excel_file(df,
                             "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/DQ_Column_Exec_Results.xlsx",
                             "DQ Column Exec RESULTS")
        elif rule_name == "List of Value / Code Check":
            passed, failed, total, result = list_of_value_code_check(df_file, data_attribute, rule_value)
            pass_percentage = (passed / total) * 100
            weighted_percentage = (pass_percentage * source_accuracy) / 100
            accuracy = accuracy + weighted_percentage
            total_accuracy = total_accuracy + 1
            df = create_col_exec_details(batch_id, job_id, data_domain, data_entity, df_file_name, data_attribute,
                                         rule_name, total, passed, failed, pass_percentage, "Accuracy",
                                         source_accuracy, weighted_percentage, batch_start_date)
            write_excel_file(df,
                             "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/DQ_Column_Exec_Results.xlsx",
                             "DQ Column Exec RESULTS")
        elif rule_name == "Data Length Check":
            passed, failed, total, result = data_length_check(df_file, data_attribute, rule_value)
            pass_percentage = (passed / total) * 100
            weighted_percentage = (pass_percentage * source_conformity) / 100
            conformity = conformity + weighted_percentage
            total_conformity = total_conformity + 1
            df = create_col_exec_details(batch_id, job_id, data_domain, data_entity, df_file_name, data_attribute,
                                         rule_name, total, passed, failed, pass_percentage, "Conformity",
                                         source_conformity, weighted_percentage, batch_start_date)
            write_excel_file(df,
                             "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/DQ_Column_Exec_Results.xlsx",
                             "DQ Column Exec RESULTS")
        elif rule_name == "Date format check":
            passed, failed, total, result = date_format_check(df_file, data_attribute, rule_logic)
            pass_percentage = (passed / total) * 100
            weighted_percentage = (pass_percentage * source_conformity) / 100
            conformity = conformity + weighted_percentage
            total_conformity = total_conformity + 1
            df = create_col_exec_details(batch_id, job_id, data_domain, data_entity, df_file_name, data_attribute,
                                         rule_name, total, passed, failed, pass_percentage, "Conformity",
                                         source_conformity, weighted_percentage, batch_start_date)
            write_excel_file(df,
                             "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/DQ_Column_Exec_Results.xlsx",
                             "DQ Column Exec RESULTS")
        elif rule_name == "Pattern Check":
            passed, failed, total, result = pattern_check(df_file, data_attribute, rule_value)
            pass_percentage = (passed / total) * 100
            weighted_percentage = (result * source_conformity) / 100
            conformity = conformity + weighted_percentage
            total_conformity = total_conformity + 1
            df = create_col_exec_details(batch_id, job_id, data_domain, data_entity, df_file_name, data_attribute,
                                         rule_name, total, passed, failed, pass_percentage, "Conformity",
                                         source_conformity, weighted_percentage, batch_start_date)
            write_excel_file(df,
                             "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/DQ_Column_Exec_Results.xlsx",
                             "DQ Column Exec RESULTS")
        elif rule_name == "Date Range Check":
            passed, failed, total, result = date_range_check(df_file, data_attribute)
            pass_percentage = (passed / total) * 100
            weighted_percentage = (pass_percentage * source_conformity) / 100
            conformity = conformity + weighted_percentage
            total_conformity = total_conformity + 1
            df = create_col_exec_details(batch_id, job_id, data_domain, data_entity, df_file_name, data_attribute,
                                         rule_name, total, passed, failed, pass_percentage, "Conformity",
                                         source_conformity, weighted_percentage, batch_start_date)
            write_excel_file(df,
                             "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/DQ_Column_Exec_Results.xlsx",
                             "DQ Column Exec RESULTS")
        elif rule_name == "Null Value Check":
            passed, failed, total, result = null_value_check(df_file, data_attribute)
            pass_percentage = (passed / total) * 100
            weighted_percentage = (pass_percentage * source_completeness) / 100
            completeness = completeness + weighted_percentage
            total_completeness = total_completeness + 1
            df = create_col_exec_details(batch_id, job_id, data_domain, data_entity, df_file_name, data_attribute,
                                         rule_name, total, passed, failed, pass_percentage, "Completeness",
                                         source_completeness, weighted_percentage, batch_start_date)
            write_excel_file(df,
                             "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/DQ_Column_Exec_Results.xlsx",
                             "DQ Column Exec RESULTS")
        elif rule_name == "Numeric Check":
            passed, failed, total, result = numeric_check(df_file, data_attribute)
            pass_percentage = (passed / total) * 100
            weighted_percentage = (pass_percentage * source_conformity) / 100
            conformity = conformity + weighted_percentage
            total_conformity = total_conformity + 1
            df = create_col_exec_details(batch_id, job_id, data_domain, data_entity, df_file_name, data_attribute,
                                         rule_name, total, passed, failed, pass_percentage, "Conformity",
                                         source_conformity, weighted_percentage, batch_start_date)
            write_excel_file(df,
                             "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/DQ_Column_Exec_Results.xlsx",
                             "DQ Column Exec RESULTS")
        elif rule_name == "Referential Integrity Check":
            passed, failed, total, result = referential_integrity_check(df_file, data_attribute)
            pass_percentage = (passed / total) * 100
            weighted_percentage = (pass_percentage * source_integrity_consistency) / 100
            int_consis = int_consis + weighted_percentage
            total_int_consis = total_int_consis + 1
            df = create_col_exec_details(batch_id, job_id, data_domain, data_entity, df_file_name, data_attribute,
                                         rule_name, total, passed, failed, pass_percentage, "Integrity & Consistency",
                                         source_integrity_consistency, weighted_percentage, batch_start_date)
            write_excel_file(df,
                             "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/DQ_Column_Exec_Results.xlsx",
                             "DQ Column Exec RESULTS")
        elif rule_name == "Business Check":
            passed, failed, total, result = business_check(df_file, data_attribute)
            pass_percentage = (passed / total) * 100
            weighted_percentage = (pass_percentage * source_completeness) / 100
            completeness = completeness + weighted_percentage
            total_completeness = total_completeness + 1
            df = create_col_exec_details(batch_id, job_id, data_domain, data_entity, df_file_name, data_attribute,
                                         rule_name, total, passed, failed, pass_percentage, "Completeness",
                                         source_completeness, weighted_percentage, batch_start_date)
            write_excel_file(df,
                             "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/DQ_Column_Exec_Results.xlsx",
                             "DQ Column Exec RESULTS")

    if total_conformity == 0:
        total_conformity = 1
    output_conformity = conformity / total_conformity
    if total_accuracy == 0:
        total_accuracy = 1
    output_accuracy = accuracy / total_accuracy
    if total_completeness == 0:
        total_completeness = 1
    output_completeness = completeness / total_completeness
    if total_validity == 0:
        total_validity = 1
    output_validity = validity / total_validity
    output_timeliness = 0
    if total_uniqueness == 0:
        total_uniqueness = 1
    output_uniqueness = uniqueness / total_uniqueness
    if total_int_consis == 0:
        total_int_consis = 1
    output_integrity_consistency = int_consis / total_int_consis

    score = output_accuracy + output_completeness + output_conformity + output_integrity_consistency + output_timeliness + output_validity + output_uniqueness

    error = "DQ checks Failed for "
    if total_conformity != 0 and output_conformity < source_conformity:
        error = error + 'Conformity, '
    if total_accuracy != 0 and output_accuracy < source_accuracy:
        error = error + 'Accuracy, '
    if total_completeness != 0 and output_completeness < source_completeness:
        error = error + 'Completeness, '
    if total_validity != 0 and output_validity < source_validity:
        error = error + 'Validity, '
    if total_uniqueness != 0 and output_uniqueness < source_uniqueness:
        error = error + 'Uniqueness, '
    if total_int_consis != 0 and output_integrity_consistency < source_integrity_consistency:
        error = error + 'Integrity & Consistency, '

    error_desc = error + 'and needs to be checked for their respective columns'
    return score, error_desc, output_conformity, output_accuracy, output_completeness, output_validity, output_timeliness, output_integrity_consistency, output_uniqueness


# Create DQ Run Details table

def create_col_exec_details(batch_id, job_id, data_domain, data_entity, table_file_id, data_attribute,
                            rule, total, passed, failed, result, dimension,
                            source_percentage, weighted_percentage, batch_start_date):
    data = {'Batch ID': batch_id,
            'Job ID': job_id,
            'Data Domain': data_domain,
            'Data Partner': data_entity,
            'Table_File Name': table_file_id,
            'Data Attribute': data_attribute,
            'Rule': rule,
            'Total Number records': total,
            'Total Number records passed': passed,
            'Total Number records failed': failed,
            'Percent DQ Passed': result,
            'DQ dim': dimension,
            'DQ Dimension Weightage': source_percentage,
            'DQ Dim Weighted Score': weighted_percentage,
            'Batch Start Date': batch_start_date,
            'Batch End Date': datetime.now()
            }
    df_col_exec_details = pd.DataFrame(data, index=[0])
    return df_col_exec_details


def create_dq_run_details(batch_id, job_id, data_domain, data_entity, table_file_id, batch_start_date, batch_end_data,
                          batch_user_name,
                          data_quality_score, dq_status):
    data = {'Batch ID': batch_id,
            'Job ID': job_id,
            'Data Domain': data_domain,
            'Data Partner': data_entity,
            'Table_File Name': table_file_id,
            'Batch Start Date': batch_start_date,
            'Batch End Date': batch_end_data,
            'Batch User Name': batch_user_name,
            'Data Quality Score': data_quality_score,
            'DQ Status': dq_status
            }
    df_dq_run_details = pd.DataFrame(data, index=[0])
    return df_dq_run_details


def create_dq_error_details(batch_id, job_id, data_domain, data_entity, table_file_id, batch_start_date, batch_end_data,
                            batch_user_name,
                            error_desc):
    data = {'Batch ID': batch_id,
            'Job ID': job_id,
            'Data Domain': data_domain,
            'Data Partner': data_entity,
            'Table_File Name': table_file_id,
            'Batch Start Date': batch_start_date,
            'Batch End Date': batch_end_data,
            'Batch User Name': batch_user_name,
            'Error Desc': error_desc
            }
    dq_error_details = pd.DataFrame(data, index=[0])
    return dq_error_details


if __name__ == '__main__':
    batch_start_date = datetime.now()
    # st.write("# Data Quality and Data Profiling Execution")
    # st.write("This page is to perform DQ checks based on the User input")
    # Main Option Menu
    choose = option_menu("Data Quality Self service portal",
                         ["Home", "DQ Config", "Data Profiling", "DQ Execution", "DQ Results", "Logout"],
                         icons=['house', 'gear', 'file-bar-graph', 'gear-fill', 'cloud-download', 'box-arrow-right'],
                         menu_icon="app-indicator", default_index=0, orientation='horizontal',
                         styles={
                             "container": {"padding": "5!important", "background-color": "#fafafa"},
                             "icon": {"color": "orange", "font-size": "18px"},
                             "nav-link": {"font-size": "12px", "text-align": "left", "margin": "0px",
                                          "--hover-color": "#eee"},
                             "nav-link-selected": {"background-color": "#07686F"},
                         }
                         )
    # Home Screen
    if choose == "Home":
        with st.sidebar:
            dat_config_choose = option_menu("Rule Approval Status",
                                            ["Approval Status"],
                                            icons=['check2-circle'],
                                            menu_icon="app-indicator", default_index=0, orientation='vertical',
                                            styles={
                                                "container": {"padding": "5!important", "background-color": "#fafafa"},
                                                "icon": {"color": "orange", "font-size": "18px"},
                                                "nav-link": {"font-size": "12px", "text-align": "left", "margin": "0px",
                                                             "--hover-color": "#eee"},
                                                "nav-link-selected": {"background-color": "#07686F"},
                                            }
                                            )
        dq_file_path = "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/"
        dq_filename = "Rule_approval_status.xlsx"
        dq_xls = pd.ExcelFile(dq_file_path + dq_filename)
        data = pd.read_excel(dq_xls, 'DQ Rule Status')
        rule_approval = data[data["Approval Status"] != "Approved"]
        st.write("Rule Approval Status")
        st.write(rule_approval)
    # DQ Config Screen
    if choose == "DQ Config":
        with st.sidebar:
            dat_config_choose = option_menu("Data Config",
                                            ["Rule Configuration", "Rule Execution", "Rule Dimension Weightage"],
                                            icons=['gear', 'gear', 'gear'],
                                            menu_icon="gear", default_index=0, orientation='vertical',
                                            styles={
                                                "container": {"padding": "5!important", "background-color": "#fafafa"},
                                                "icon": {"color": "orange", "font-size": "18px"},
                                                "nav-link": {"font-size": "12px", "text-align": "left", "margin": "0px",
                                                             "--hover-color": "#eee"},
                                                "nav-link-selected": {"background-color": "#07686F"},
                                            }
                                            )
        # DQ Config --> Rule Configuration screen
        if dat_config_choose == "Rule Configuration":
            with st.sidebar:
                rule_config_choose = option_menu("Rule Configuration",
                                                 ["Add DQ Rule", "Edit/Modify Existing DQ Rule", "Delete DQ Rule"],
                                                 icons=['gear', 'gear', 'gear'],
                                                 menu_icon="gear", default_index=0, orientation='vertical',
                                                 styles={
                                                     "container": {"padding": "5!important",
                                                                   "background-color": "#fafafa"},
                                                     "icon": {"color": "orange", "font-size": "18px"},
                                                     "nav-link": {"font-size": "12px", "text-align": "left",
                                                                  "margin": "0px",
                                                                  "--hover-color": "#eee"},
                                                     "nav-link-selected": {"background-color": "#07686F"},
                                                 }
                                                 )
            dq_file_path = "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/"
            dq_filename = "Data_Quality_Design.xlsx"
            dq_xls = pd.ExcelFile(dq_file_path + dq_filename)
            data = pd.read_excel(dq_xls, 'DQ Rule Master')
            # DQ Config --> Rule Configuration screen --> Add DQ Rule screen
            if rule_config_choose == "Add DQ Rule":
                add_rule_id = st.text_input('Rule ID', '')
                add_rule_name = st.text_input('Rule Name', '')
                add_rule_desc = st.text_input('Rule Description', '')
                add_rule_type = st.selectbox(
                    'Rule Type',
                    ('Technical', 'Operational', 'Functional'))
                add_rule_dim = st.selectbox(
                    'Rule Dimension',
                    ('Conformity', 'Accuracy', 'Completeness', 'Validity', 'uniqueness', 'Integrity'))
                add_rule_status = st.radio(
                    'Rule Status', ('Active', 'Inactive')
                )
                col1, col2 = st.columns([1, 1])
                with col1:
                    if st.button("Save"):
                        st.info("Your data has been saved for later use")
                with col2:
                    if st.button("Submit for Approval"):
                        st.info('Request submitted for Approval')
                        if add_rule_status == "Active":
                            add_rule_status1 = 'Y'
                        elif add_rule_status == "Inactive":
                            add_rule_status1 = 'N'
                        add_approval_status = 'Waiting for Admin Approval'
                        data = {
                            "Approval Status": add_approval_status,
                            "Rule Status": add_rule_status1,
                            "Rule ID": add_rule_id,
                            "Rule Name": add_rule_name,
                            "Rule Description": add_rule_desc,
                            "Rule Type": add_rule_type,
                            "Rule Dimension": add_rule_dim
                        }
                        df_add_rules = pd.DataFrame(data, index=[0])
                        write_excel_file(df_add_rules,
                                         "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/Rule_approval_status.xlsx",
                                         "DQ Rule Status")
# DQ Config --> Rule Configuration screen --> Edit/Modify Existing DQ Rule screen
            if rule_config_choose == "Edit/Modify Existing DQ Rule":
                data_name = data['Rule Name'].unique()
                sel_data_name = st.selectbox(
                    'Select the Rule Name ', data_name)
                data_rule_name = data[data["Rule Name"] == sel_data_name]
                st.write(data_rule_name)
                st.write("update the columns below")
                rule_type = st.selectbox(
                    'Select the Rule Type ',
                    ('Technical', 'Operational', 'Functional')
                )
                rule_dim = st.selectbox(
                    'Select the Rule Dimension ',
                    ('Completeness', 'Conformity', 'Accuracy', 'Uniqueness', 'Validity', 'Timeliness',
                     'Integrity & Consistency')
                )
                rule_type = st.selectbox(
                    'Select the Rule Status ',
                    ('Active', 'Inactive')
                )
                col1, col2 = st.columns([1, 1])
                with col1:
                    if st.button("Save"):
                        st.info("Your data has been saved for later use")
                with col2:
                    if st.button("Submit for Approval"):
                        st.info('Request submitted for Approval')
# DQ Config --> Rule Configuration screen --> Delete DQ Rule screen
            if rule_config_choose == "Delete DQ Rule":
                data_name = data['Rule Name'].unique()
                sel_data_name = st.selectbox(
                    'Select the Rule Name ', data_name)
                col1, col2 = st.columns([1, 1])
                with col1:
                    if st.button("Save"):
                        st.info("Your data has been saved for later use")
                with col2:
                    if st.button("Submit for Approval"):
                        st.info('Request submitted for Approval')
# DQ Config --> Rule Execution screen
        if dat_config_choose == "Rule Execution":
            dq_file_path = "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/"
            dq_filename = "Data_Quality_Design.xlsx"
            dq_xls = pd.ExcelFile(dq_file_path + dq_filename)
            data = pd.read_excel(dq_xls, 'Metadata Repo')
            data_domain = data['Data Domain'].unique()
            data_domain_value = st.selectbox(
                'Data Domain',
                data_domain)

            data_element = data[data["Data Domain"] == data_domain_value]['Data Element'].values
            data_element = pd.unique(data_element)
            data_element_values = st.selectbox(
                'Data Partner', data_element)

            source = st.radio(
                'Source File/ Table', ('File', 'Table')
            )

            if source == "File":
                data_file = data[data["Data Element"] == data_element_values]['Data File'].values
                data_file_values = st.selectbox(
                    'Data File', data_file)
            else:
                data_file = data[data["Data Element"] == data_element_values]['Data Table'].values
                data_file_values = st.selectbox(
                    'Data Table', data_file)

            data_file_path = "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/"
            dq_profile_filename = "Data_Quality_Design.xlsx"
            xls = pd.ExcelFile(data_file_path + dq_profile_filename)
            df_metadata_repo = pd.read_excel(xls, 'Metadata Repo')
            data_table_file_id = df_metadata_repo[(df_metadata_repo["Data Domain"] == data_domain_value)
                                                  & (df_metadata_repo["Data Element"] == data_element_values)
                                                  & (df_metadata_repo["Data File"] == data_file_values)][
                "Table_File ID"].values.tolist()
            df_data_req_repo = pd.read_excel(xls, 'Data Requirement Repo')
            df_req_fields = df_data_req_repo[(df_data_req_repo["Table_File ID"].isin(data_table_file_id))
            ]['Data Attribute']
            df_req_fields = df_req_fields.unique()
            df_req_fields_val = st.selectbox(
                'Columns', df_req_fields
            )
            rule_id = st.text_input('Rule Name', '')
            st.button("Add")
        # DQ Config --> Rule Dimension Weightage screen
        if dat_config_choose == "Rule Dimension Weightage":
            with st.sidebar:
                rule_dim_choose = option_menu("Rule Dimension Weightage",
                                              ["Add Dimension Weightage", "Edit/Modify Existing Dimension Weightage",
                                               "Delete Dimension Weightage"],
                                              icons=['gear', 'gear', 'gear'],
                                              menu_icon="gear", default_index=0, orientation='vertical',
                                              styles={
                                                  "container": {"padding": "5!important",
                                                                "background-color": "#fafafa"},
                                                  "icon": {"color": "orange", "font-size": "18px"},
                                                  "nav-link": {"font-size": "12px", "text-align": "left",
                                                               "margin": "0px",
                                                               "--hover-color": "#eee"},
                                                  "nav-link-selected": {"background-color": "#07686F"},
                                              }
                                              )
            dq_file_path = "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/"
            dq_filename = "Data_Quality_Design.xlsx"
            dq_xls = pd.ExcelFile(dq_file_path + dq_filename)
            data = pd.read_excel(dq_xls, 'Metadata Repo')
            if (
                    rule_dim_choose == "Add Dimension Weightage" or rule_dim_choose == "Edit/Modify Existing Dimension Weightage"):
                data_domain = data['Data Domain'].unique()
                data_domain_value = st.selectbox(
                    'Data Domain',
                    data_domain)

                data_element = data[data["Data Domain"] == data_domain_value]['Data Element'].values
                data_element = pd.unique(data_element)
                data_element_values = st.selectbox(
                    'Data Partner', data_element)

                source = st.radio(
                    'Source File/ Table', ('File', 'Table')
                )

                if source == "File":
                    data_file = data[data["Data Element"] == data_element_values]['Data File'].values
                    data_file_values = st.selectbox(
                        'Data File', data_file)
                else:
                    data_file = data[data["Data Element"] == data_element_values]['Data Table'].values
                    data_file_values = st.selectbox(
                        'Data Table', data_file)
                slider_val = 100
                col1, col2 = st.columns([1, 1])
                with col1:
                    accuracy_wgt = st.slider('DQ- Accuracy Weightage (%)', min_value=0, max_value=100)
                slider_val = slider_val - accuracy_wgt
                with col2:
                    conformity_wgt = st.slider('DQ- Conformity Weightage (%)', min_value=0, max_value=slider_val)
                slider_val = slider_val - conformity_wgt
                col1, col2 = st.columns([1, 1])
                with col1:
                    completeness_wgt = st.slider('DQ- Completeness Weightage (%)', min_value=0, max_value=slider_val)
                slider_val = slider_val - completeness_wgt
                with col2:
                    validity_wgt = st.slider('DQ- Validity Weightage (%)', min_value=0, max_value=slider_val)
                slider_val = slider_val - validity_wgt
                col1, col2 = st.columns([1, 1])
                with col1:
                    timeliness_wgt = st.slider('DQ- Timeliness Weightage (%)', min_value=0, max_value=slider_val)
                slider_val = slider_val - timeliness_wgt
                with col2:
                    int_con_wgt = st.slider('DQ- Integrity & Consistency Weightage (%)', min_value=0,
                                            max_value=slider_val)
                slider_val = slider_val - int_con_wgt
                unique_wgt = st.slider('DQ- Uniqueness Weightage (%)', min_value=0, max_value=slider_val)
                col1, col2 = st.columns([1, 1])
                with col1:
                    st.button("Save")
                with col2:
                    st.button("Submit")
            if rule_dim_choose == "Delete Dimension Weightage":
                data_domain = data['Data Domain'].unique()
                data_domain_value = st.selectbox(
                    'Data Domain',
                    data_domain)

                data_element = data[data["Data Domain"] == data_domain_value]['Data Element'].values
                data_element = pd.unique(data_element)
                data_element_values = st.selectbox(
                    'Data Partner', data_element)

                source = st.radio(
                    'Source File/ Table', ('File', 'Table')
                )

                if source == "File":
                    data_file = data[data["Data Element"] == data_element_values]['Data File'].values
                    data_file_values = st.selectbox(
                        'Data File', data_file)
                else:
                    data_file = data[data["Data Element"] == data_element_values]['Data Table'].values
                    data_file_values = st.selectbox(
                        'Data Table', data_file)
                col1, col2 = st.columns([1, 1])
                with col1:
                    if st.button("Save"):
                        st.info("Saved for later use")
                with col2:
                    if st.button("Delete"):
                        st.info("Submitted for Approval")

    # DQ Execution screen
    if choose == "DQ Execution":
        with st.sidebar:
            rule_config_choose = option_menu("DQ Execution",
                                             ["Table/File Configuration", "Batch Configuration",
                                              "Source Target Configuration", "Rule Parameters"],
                                             icons=['gear', 'gear', 'gear', 'gear'],
                                             menu_icon="app-indicator", default_index=0, orientation='vertical',
                                             styles={
                                                 "container": {"padding": "5!important", "background-color": "#fafafa"},
                                                 "icon": {"color": "orange", "font-size": "18px"},
                                                 "nav-link": {"font-size": "12px", "text-align": "left",
                                                              "margin": "0px",
                                                              "--hover-color": "#eee"},
                                                 "nav-link-selected": {"background-color": "#07686F"},
                                             }
                                             )
        # DQ Execution --> Table/File Configuration screen
        if rule_config_choose == "Table/File Configuration":
            table_file_id = st.text_input('Table File ID', '')
            data_domain = st.text_input('Data Domain', '')
            data_partner = st.text_input('Data Partner', '')
            source = st.radio(
                'Source File/ Table', ('File', 'Table')
            )
            if source == "File":
                data_file_core = st.text_input('Data File Core', '')
                data_file_name = st.text_input('Data File Name', '')
                data_file_path = st.text_input('Data File Path', '')
                data_file_Delimiter = st.text_input('Data File Delimiter', '')
            else:
                data_table_db_name = st.text_input('Data Table Database Name', '')
                data_table_schema_name = st.text_input('Data Table Schema Name', '')
                data_table_name = st.text_input('Data Table Name', '')
            col1, col2 = st.columns([1, 1])
            with col1:
                if st.button("Save"):
                    st.info("Saved for later use")
            with col2:
                if st.button("Submit"):
                    st.info("Submitted")
# DQ Execution --> Batch Configuration screen
        if rule_config_choose == "Batch Configuration":
            batch_id = st.text_input('Batch ID', '')
            batch_type = st.selectbox(
                'Batch Type',
                ('Schedule', 'Manual', 'OneTime')
            )
            batch_name = st.text_input('Batch Name', '')
            batch_description = st.text_input('Batch Description', '')
            batch_frequency = st.selectbox(
                'Batch Frequency',
                ('Daily', 'Monthly', 'Weekly')
            )
            batch_enable = st.radio(
                'Enable Batch', ('Yes', 'No')
            )
            # data = pd.read_csv("C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/Data_Quality_Design.xlsx", sep=",")
            dq_file_path = "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/"
            dq_filename = "Data_Quality_Design.xlsx"
            dq_xls = pd.ExcelFile(dq_file_path + dq_filename)
            data = pd.read_excel(dq_xls, 'Metadata Repo')

            data_domain = data['Data Domain'].unique()
            data_domain_value = st.selectbox(
                'Data Domain',
                data_domain)

            data_element = data[data["Data Domain"] == data_domain_value]['Data Element'].values
            data_element = pd.unique(data_element)
            data_element_values = st.selectbox(
                'Data Partner', data_element)

            source = st.radio(
                'Source File/ Table', ('File', 'Table')
            )

            if source == "File":
                data_file = data[data["Data Element"] == data_element_values]['Data File'].values
                data_file_values = st.multiselect(
                    'Data File', data_file)
            else:
                data_file = data[data["Data Element"] == data_element_values]['Data Table'].values
                data_file_values = st.multiselect(
                    'Data Table', data_file)
            status = True
            if st.button('Run'):
                # my_bar = st.success("DQ process is in Progress")
                data_domain = data_domain_value
                data_entity = data_element_values
                # data_file = data_file_values
                batch_id = int(batch_id)

                data_file_df = pd.DataFrame(data_file_values)
                job_id = 1
                file_passed_cnt = 0
                file_failed_cnt = 0
                for data_file_val in data_file_df.index:
                    data_file = data_file_df[0][data_file_val]
                    # File which contains the dq rules and other tables
                    data_file_path = "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/"
                    dq_profile_filename = "Data_Quality_Design.xlsx"
                    xls = pd.ExcelFile(data_file_path + dq_profile_filename)
                    df_metadata_repo = pd.read_excel(xls, 'Metadata Repo')
                    df_data_req_repo = pd.read_excel(xls, 'Data Requirement Repo')
                    df_dq_rule_master = pd.read_excel(xls, 'DQ Rule Master')
                    df_dq_weightage = pd.read_excel(xls, 'Data Req - DQ Rule Weightage Ma')
                    data_table_file_id = df_metadata_repo[(df_metadata_repo["Data Domain"] == data_domain)
                                                          & (df_metadata_repo["Data Element"] == data_entity)
                                                          & (df_metadata_repo["Data File"] == data_file)][
                        "Table_File ID"].values.tolist()

                    df_req = df_data_req_repo[(df_data_req_repo["Table_File ID"].isin(data_table_file_id))
                                              & (df_data_req_repo["DQ Applicablity Flag"] == 'Y')][
                        ['Rule ID', 'Data Attribute', 'Rule Value', 'Data type Format', 'Rule Logic']]
                    df_req_list = list(set(df_req['Rule ID'].values.tolist()))
                    df_attributes = pd.merge(df_req, df_dq_rule_master, how='inner', left_on=['Rule ID'],
                                             right_on=['Rule ID'])
                    df_attributes = df_attributes.sort_values(by='Data Attribute')
                    data_file_path = df_metadata_repo[(df_metadata_repo["Data Domain"] == data_domain)
                                                      & (df_metadata_repo["Data Element"] == data_entity)
                                                      & (df_metadata_repo["Data File"] == data_file)][
                        "Data File Path"].values.tolist()[0]
                    data_file_dlm = df_metadata_repo[(df_metadata_repo["Data Domain"] == data_domain)
                                                     & (df_metadata_repo["Data Element"] == data_entity)
                                                     & (df_metadata_repo["Data File"] == data_file)][
                        "Data File Delimeter"].values.tolist()[0]
                    df = read_csv_file(data_file_path + data_file, data_file_dlm)
                    data_qlt_df = get_data_profiling_stats(df, batch_start_date)
                    write_excel_file(data_qlt_df,
                                     "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/Data_Profiling_Results.xlsx",
                                     "DQ Profiling Results")
                    for i in data_table_file_id:
                        # print(i)
                        data_file_path = df_metadata_repo[(df_metadata_repo["Data Domain"] == data_domain)
                                                          & (df_metadata_repo["Data Element"] == data_entity)
                                                          & (df_metadata_repo["Data File"] == data_file)][
                            "Data File Path"].values.tolist()[0]
                        data_file_dlm = df_metadata_repo[(df_metadata_repo["Data Domain"] == data_domain)
                                                         & (df_metadata_repo["Data Element"] == data_entity)
                                                         & (df_metadata_repo["Data File"] == data_file)][
                            "Data File Delimeter"].values.tolist()[0]
                        df_file_name = data_file
                        df_file = read_csv_file(data_file_path + data_file, data_file_dlm)
                        # Calculate the source weightages
                        data_quality_score, error_desc, output_conformity, output_accuracy, output_completeness, output_validity, output_timeliness, output_integrity_consistency, output_uniqueness = calculate_score(
                            df_file_name, df_attributes, df_file, df_dq_weightage)

                        if data_quality_score > 90:
                            dq_status = "Passed"
                            file_passed_cnt = file_passed_cnt + 1
                            dq_run_details = create_dq_run_details(batch_id, job_id, data_domain, data_entity,
                                                                   data_file,
                                                                   batch_start_date,
                                                                   datetime.now(),
                                                                   "EY Team", data_quality_score,
                                                                   dq_status)
                            write_excel_file(dq_run_details,
                                             "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/DQ_run_Results.xlsx",
                                             "DQ RESULTS")
                            # st.write("DQ Results for", data_file)
                            # st.write(dq_run_details)
                        else:
                            dq_status = "Failed"
                            file_failed_cnt = file_failed_cnt + 1
                            dq_error_details = create_dq_error_details(batch_id, job_id, data_domain, data_entity,
                                                                       data_file,
                                                                       batch_start_date,
                                                                       datetime.now(),
                                                                       "EY Team", error_desc)
                            dq_run_details = create_dq_run_details(batch_id, job_id, data_domain, data_entity,
                                                                   data_file,
                                                                   batch_start_date,
                                                                   datetime.now(),
                                                                   "EY Team", data_quality_score,
                                                                   dq_status)
                            # st.write("DQ Results for ", data_file)
                            write_excel_file(dq_run_details,
                                             "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/DQ_run_Results.xlsx",
                                             "DQ RESULTS")
                            # st.write(dq_run_details)
                            # st.write("DQ Error Results for ", data_file)
                            write_excel_file(dq_error_details,
                                             "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/DQ_run_Results_error.xlsx",
                                             "DQ ERRORS")
                            # st.write(dq_error_details)
                        job_id = job_id + 1
                col1, col2, col3 = st.columns([1, 1, 1])
                with col1:
                    st.metric(
                        "Total Files/Tables Processed", job_id - 1
                    )
                with col2:
                    st.metric(
                        "Total Files/Tables Passed", file_passed_cnt
                    )
                with col3:
                    st.metric(
                        "Total Files/Tables Failed", file_failed_cnt
                    )
        # DQ Execution --> Source Target Configuration screen
        dq_file_path = "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/"
        dq_filename = "Data_Quality_Design.xlsx"
        dq_xls = pd.ExcelFile(dq_file_path + dq_filename)
        data = pd.read_excel(dq_xls, 'Metadata Repo')

        if rule_config_choose == "Source Target Configuration":
            st.write("Source")
            col1, col2, col3, col4 = st.columns([1, 1, 1, 1])
            with col1:
                data_domain = data['Data Domain'].unique()
                data_domain_value = st.selectbox(
                    'Source Data Domain',
                    data_domain)
            with col2:
                data_element = data[data["Data Domain"] == data_domain_value]['Data Element'].values
                data_element = pd.unique(data_element)
                data_element_values = st.selectbox(
                    'SourceData Partner', data_element)
            with col3:
                source = st.selectbox(
                    'Source File/ Table', ('File', 'Table')
                )
            with col4:
                if source == "File":
                    data_file = data[data["Data Element"] == data_element_values]['Data File'].values
                    data_file_values = st.selectbox(
                        'Source Data File', data_file)
                else:
                    data_file = data[data["Data Element"] == data_element_values]['Data Table'].values
                    data_file_values = st.selectbox(
                        'Source Data Table', data_file)
            st.write("Target")
            col5, col6, col7, col8 = st.columns([1, 1, 1, 1])
            with col5:
                data_domain_tgt = data['Data Domain'].unique()
                data_domain_value_tgt1 = st.selectbox(
                    'Target Data Domain',
                    data_domain_tgt)
            with col6:
                data_element_tgt = data[data["Data Domain"] == data_domain_value_tgt1]['Data Element'].values
                data_element_tgt = pd.unique(data_element_tgt)
                data_element_values_tgt = st.selectbox(
                    'Target Data Partner', data_element_tgt)
            with col7:
                source_tgt = st.selectbox(
                    'Target File/ Table', ('File', 'Table')
                )
            with col8:
                if source_tgt == "File":
                    data_file_tgt = data[data["Data Element"] == data_element_values_tgt]['Data File'].values
                    data_file_values_tgt = st.selectbox(
                        'Target Data File', data_file_tgt)
                else:
                    data_file_tgt = data[data["Data Element"] == data_element_values_tgt]['Data Table'].values
                    data_file_values_tgt = st.selectbox(
                        'Target Data Table', data_file_tgt)
            st.button("Map")
        # DQ Execution --> Rule Parameters screen
        if rule_config_choose == "Rule Parameters":
            data_domain = data['Data Domain'].unique()
            data_domain_value = st.selectbox(
                'Data Domain',
                data_domain)

            data_element = data[data["Data Domain"] == data_domain_value]['Data Element'].values
            data_element = pd.unique(data_element)
            data_element_values = st.selectbox(
                'Data Partner', data_element)

            source = st.radio(
                'Source File/ Table', ('File', 'Table')
            )

            if source == "File":
                data_file = data[data["Data Element"] == data_element_values]['Data File'].values
                data_file_values = st.selectbox(
                    'Data File', data_file)
            else:
                data_file = data[data["Data Element"] == data_element_values]['Data Table'].values
                data_file_values = st.selectbox(
                    'Data Table', data_file)

            data_file_path = "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/"
            dq_profile_filename = "Data_Quality_Design.xlsx"
            xls = pd.ExcelFile(data_file_path + dq_profile_filename)
            df_metadata_repo = pd.read_excel(xls, 'Metadata Repo')
            data_table_file_id = df_metadata_repo[(df_metadata_repo["Data Domain"] == data_domain_value)
                                                  & (df_metadata_repo["Data Element"] == data_element_values)
                                                  & (df_metadata_repo["Data File"] == data_file_values)][
                "Table_File ID"].values.tolist()
            df_data_req_repo = pd.read_excel(xls, 'Data Requirement Repo')
            df_req_fields = df_data_req_repo[(df_data_req_repo["Table_File ID"].isin(data_table_file_id))
            ]['Data Attribute']
            df_req_fields = df_req_fields.unique()
            df_req_fields_val = st.selectbox(
                'Columns', df_req_fields
            )
            prm_val = st.selectbox(
                'Select the parameter',
                ('Data Format', 'Rule Value', 'Rule Logic')
            )
            if prm_val == "Data Format":
                date_format = st.text_input('Enter the source Date Format', '')
            if prm_val == "Rule Value":
                rule_value = st.text_input('Enter the length or list of values - separated with comma(,)', '')
            if prm_val == "Rule Logic":
                date_format = st.text_input('Enter the transformation logic like select * from table', '')
            st.button("Submit")
    if choose == "Data Profiling":
        data_results_xls = pd.ExcelFile(
            "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/Data_Profiling_Results.xlsx")
        data_results = pd.read_excel(data_results_xls, 'DQ Profiling Results')
        select_column = st.selectbox(
            'What would you like to analyse by?',
            ('Data Domain', 'Batch ID', 'Data Partner', 'Table_File Name', 'Date Range')
        )
        if select_column == "Batch ID":
            data_batch_value = data_results["Batch ID"].unique()
            data_file_values = st.multiselect(
                'Select Values', data_batch_value)
            if st.button("Run"):
                data_file_df = pd.DataFrame(data_file_values)
                for data_file_val in data_file_df.index:
                    data_file = data_file_df[0][data_file_val]
                    data_batch = data_results[(data_results["Batch ID"] == data_file)]
                    st.write("Results of Batch:", str(data_file))
                    st.write(data_batch)

        if select_column == "Data Domain":
            data_batch_value = data_results["Data Domain"].unique()
            data_file_values = st.multiselect(
                'Select Values', data_batch_value)
            if st.button("Run"):
                data_file_df = pd.DataFrame(data_file_values)
                for data_file_val in data_file_df.index:
                    data_file = data_file_df[0][data_file_val]
                    data_batch = data_results[(data_results["Data Domain"] == data_file)]
                    st.write("Results of Data Domain:", str(data_file))
                    st.write(data_batch)

        if select_column == "Data Partner":
            data_batch_value = data_results["Data Partner"].unique()
            data_file_values = st.multiselect(
                'Select Values', data_batch_value)
            if st.button("Run"):
                data_file_df = pd.DataFrame(data_file_values)
                for data_file_val in data_file_df.index:
                    data_file = data_file_df[0][data_file_val]
                    data_batch = data_results[(data_results["Data Partner"] == data_file)]
                    st.write("Results of Data Partner:", str(data_file))
                    st.write(data_batch)

        if select_column == "Table_File Name":
            data_batch_value = data_results["File Name"].unique()
            data_file_values = st.multiselect(
                'Select Values', data_batch_value)
            if st.button("Run"):
                data_file_df = pd.DataFrame(data_file_values)
                for data_file_val in data_file_df.index:
                    data_file = data_file_df[0][data_file_val]
                    data_batch = data_results[(data_results["File Name"] == data_file)]
                    st.write("Results of Data Partner:", str(data_file))
                    st.write(data_batch)
    if choose == "DQ Results":
        with st.sidebar:
            rule_config_choose = option_menu("DQ Execution",
                                             ["DQ Results", "DQ Errors"],
                                             icons=['cloud-download', 'cloud-download'],
                                             menu_icon="cloud-download", default_index=0, orientation='vertical',
                                             styles={
                                                 "container": {"padding": "5!important", "background-color": "#fafafa"},
                                                 "icon": {"color": "orange", "font-size": "18px"},
                                                 "nav-link": {"font-size": "12px", "text-align": "left",
                                                              "margin": "0px",
                                                              "--hover-color": "#eee"},
                                                 "nav-link-selected": {"background-color": "#07686F"},
                                             }
                                             )
        if rule_config_choose == "DQ Results":
            data_results_xls = pd.ExcelFile(
                "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/DQ_run_Results.xlsx")
            data_results = pd.read_excel(data_results_xls, 'DQ RESULTS')
            # df_excel = pd.read_excel("C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/DQ_run_Results.xlsx, sheet_name="DQ RESULTS")
            # data = pd.read_csv("C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/Metadata_Repo.txt", sep=",")

            # if st.button("Bar Chart"):
            st.write(data_results)
            groupby_column = st.selectbox(
                'What would you like to analyse by?',
                ('Data Domain', 'Batch ID', 'Data Partner', 'Table_File Name', 'Date Range')
            )
            if (groupby_column == "Data Domain") or (groupby_column == "Batch ID") or (
                    groupby_column == "Data Partner") or (groupby_column == "Table_File Name"):
                st.write(groupby_column)
                output_columns = ['Data Quality Score']
                df_grouped = data_results.groupby(by=[groupby_column], as_index=False)[output_columns].sum()
                # chart_data = pd.DataFrame(
                # data_results,
                # columns=["Table_File ID", "Data Quality Score"])
                st.bar_chart(data=df_grouped, x=groupby_column)
            elif (groupby_column == "Date Range"):
                start_date = st.date_input(
                    "Start Date",
                    date.today() - timedelta(days=1))
                # start_date = pd.to_datetime(start_date)
                end_date = st.date_input(
                    "End Date",
                    date.today() + timedelta(days=1))
                date_range = data_results[(data_results['Batch Start Date'] >= np.datetime64(start_date)) & (
                        data_results['Batch End Date'] <= np.datetime64(end_date))]
                st.write(date_range)
                groupby_column = st.selectbox(
                    'What would you like to analyse by?',
                    ('Data Domain', 'Batch ID', 'Data Partner', 'Table_File Name')
                )
                st.write(groupby_column)
                output_columns = ['Data Quality Score']
                df_grouped = date_range.groupby(by=[groupby_column], as_index=False)[output_columns].sum()
                # chart_data = pd.DataFrame(
                # data_results,
                # columns=["Table_File ID", "Data Quality Score"])
                st.bar_chart(data=df_grouped, x=groupby_column)
        if rule_config_choose == "DQ Errors":
            data_results_xls = pd.ExcelFile(
                "C:/Users/EE122VC/OneDrive - EY/Ranjit/Project/CCB/DQ test files/Files/DQ_run_Results_error.xlsx")
            data_results = pd.read_excel(data_results_xls, 'DQ ERRORS')
            st.write(data_results)

