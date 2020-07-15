from ProjectBevan.utilities import parse_datetime, verbose_print, progress_bar
from ProjectBevan.schema import create_database
from multiprocessing import Pool, cpu_count
from tqdm import tqdm
from typing import List
import sqlite3 as sql
import pandas as pd
import re
import os


def _indexed_datetime(x):
    return x[0], parse_datetime(x[1])


def chunker(seq, size):
    # from http://stackoverflow.com/a/434328
    return (seq[pos:pos + size] for pos in range(0, len(seq), size))


def _re_search_df(pattern: str,
                  x: str,
                  group_idx: int = 0):
    match = re.search(pattern=pattern,
                      string=x)
    if match is None:
        return None
    if len(match.groups()) == 1:
        return match.groups()[0]
    return match.groups()[group_idx]


def _rename(df: pd.DataFrame,
            additional_mappings: dict or None = None):
    df = df.rename({"PATIENT_ID": "patient_id",
                    "REQUEST_LOCATION": "request_location"},
                   axis=1)
    if additional_mappings is not None:
        df = df.rename(additional_mappings, axis=1)
    return df


def _get_date_time(df: pd.DataFrame,
                   col_name: str = "TEST_DATE",
                   new_date_name: str = "test_date",
                   new_time_name: str = "test_time"):
    idx_values = df.reset_index()[["index", col_name]].values
    idx_values = [(x[0], x[1]) for x in idx_values]
    pool = Pool(cpu_count())
    parsed_datetime = sorted(pool.map(_indexed_datetime, idx_values), key=lambda x: x[0])
    pool.close()
    pool.join()
    df[new_date_name] = [x[1].get("date") for x in parsed_datetime]
    df[new_time_name] = [x[1].get("time") for x in parsed_datetime]
    df.drop(col_name, axis=1, inplace=True)
    return df


class Populate:
    def __init__(self,
                 database_path: str,
                 data_path: str,
                 verbose: bool = True,
                 died_events: List[str] or None = None,
                 path_files: List[str] or None = None,
                 micro_files: List[str] or None = None,
                 comorbid_files: List[str] or None = None,
                 patient_files: List[str] or None = None,
                 haem_files: List[str] or None = None,
                 critcare_files: List[str] or None = None,
                 radiology_files: List[str] or None = None,
                 events_files: List[str] or None = None):
        self.verbose = verbose
        self.vprint = verbose_print(verbose)
        create_database(database_path, overwrite=True)
        self._connection = sql.connect(database_path)
        self._curr = self._connection.cursor()
        self.data_path = data_path
        assert os.path.isdir(self.data_path), f"{data_path} is not a valid directory"
        self.path_files = path_files
        self.micro_files = micro_files
        self.comorbid_files = comorbid_files
        self.patient_files = patient_files
        self.haem_files = haem_files
        self.critcare_files = critcare_files
        self.radiology_files = radiology_files
        self.events_files = events_files
        if self.path_files is None:
            self.path_files = ["LFT",
                               "ABG",
                               "ACE",
                               "AntiXa",
                               "BgaPOCT",
                               "CoagScr",
                               "Covid19Ab",
                               "CRP",
                               "Ddimer",
                               "EPS",
                               "FBC",
                               "Ferritin",
                               "GlucoseRand",
                               "HbA1c",
                               "HsTrop",
                               "ImmGlob",
                               "LDH",
                               "LFT",
                               "Lip",
                               "LipF",
                               "ParaProt",
                               "ProCalc",
                               "TCC",
                               "TFSat",
                               "UandE",
                               "VitD"]
        if self.micro_files is None:
            self.micro_files = ["AsperELISA",
                                "AsperPCR",
                                "BCult",
                                "RESPL"]
        if self.comorbid_files is None:
            self.comorbid_files = ["CoMorbid"]
        if self.haem_files is None:
            self.haem_files = ["CompAlt",
                               "CompClass"]
        if self.patient_files is None:
            self.patient_files = ["People",
                                  "Outcomes",
                                  "Covid19"]
        if self.critcare_files is None:
            self.critcare_files = ["CritCare"]
        if self.radiology_files is None:
            self.radiology_files = ["XRChest",
                                    "CTangio"]
        if self.events_files is None:
            self.events_files = ["Outcomes"]
        self.died_events = died_events
        if self.died_events is None:
            self.died_events = ['Died - DEATHS INCLUDING STILLBIRTHS',
                                'Died In Dept.',
                                'Died - USUAL PLACE OF RESIDENCE',
                                'Died - NHS HOSP OTHER PROV - GENERAL']
        self._all_files_present()

    def _all_files_present(self):
        files = [os.path.splitext(f)[0] for f in os.listdir(self.data_path)
                 if os.path.isfile(os.path.join(self.data_path, f))]
        for group in [self.events_files, self.patient_files, self.haem_files,
                      self.radiology_files, self.critcare_files, self.comorbid_files,
                      self.micro_files, self.path_files]:
            for expected_file in group:
                assert expected_file in files, f"{expected_file} missing from data path!"

    def _get_path(self,
                  file_basename: str):
        return os.path.join(self.data_path, f"{file_basename}.csv")

    def _insert(self,
                df: pd.DataFrame,
                table_name: str):
        if not self.verbose:
            df.to_sql(name=table_name, con=self._connection, if_exists="append", index=False)
            return
        chunk_size = int(df.shape[0]/10)
        with tqdm(total=df.shape[0]) as pbar:
            for chunk in chunker(df, chunk_size):
                chunk.to_sql(name=table_name, con=self._connection, if_exists="append", index=False)
                pbar.update(chunk_size)

    def _pathology(self):
        self.vprint("---- Populating Pathology Table ----")
        for file in self.path_files:
            self.vprint(f"Processing {file}....")
            df = pd.read_csv(self._get_path(file), low_memory=False)
            df.drop(["AGE", "GENDER", "ADMISSION_DATE"], axis=1, inplace=True)
            df = _get_date_time(df)
            df = df.melt(id_vars=["PATIENT_ID", "REQUEST_LOCATION", "test_date", "test_time"],
                         var_name="test_name",
                         value_name="test_result")
            df["valid"] = df.test_result.apply(lambda x: int(x != "Issue with result"))
            df["test_category"] = file
            df = _rename(df)
            self._insert(df=df, table_name="Pathology")

    def _process_micro_df(self,
                          df: pd.DataFrame,
                          sample_type_pattern: str,
                          result_pattern: str,
                          test_name: str):
        df.drop(["AGE", "GENDER", "ADMISSION_DATE"], axis=1, inplace=True)
        df = _get_date_time(df)
        # pull out the sample type
        df["sample_type"] = df.TEXT.apply(lambda x: _re_search_df(pattern=sample_type_pattern, x=x, group_idx=0))
        # Pull out result
        df["test_result"] = df.TEXT.apply(lambda x: _re_search_df(pattern=result_pattern, x=x, group_idx=1))
        df["test_name"] = test_name
        df["valid"] = df.TEXT.apply(lambda x: int(x != "Issue with result"))
        df = _rename(df, {"TEXT": "raw_text"})
        self._insert(df=df, table_name="Microbiology")

    def _microbiology(self):
        self.vprint("---- Populating Microbiology Table ----")

        # AsperELISA ----------------------------
        self.vprint("...processing Aspergillus ELISA results")
        df = pd.read_csv(self._get_path("AsperELISA"), low_memory=False)
        sample_type_pattern = 'Specimen received: ([\w\d\s\(\)\[\]]+) Aspergillus ELISA'
        result_pattern = "Aspergillus Antigen \(Galactomannan\) ([\w\d\s\(\)\[\]]+)"
        self._process_micro_df(df=df,
                               sample_type_pattern=sample_type_pattern,
                               result_pattern=result_pattern,
                               test_name="AsperELISA")
        # AsperPCR ----------------------------
        self.vprint("...processing Aspergillus PCR results")
        df = pd.read_csv(self._get_path("AsperPCR"), low_memory=False)
        sample_type_pattern = 'Specimen received: ([\w\d\s\(\)\[\]]+) Aspergillus PCR'
        result_pattern = "PCR\s(DNA\s[Not]*\sDetected)"
        self._process_micro_df(df=df,
                               sample_type_pattern=sample_type_pattern,
                               result_pattern=result_pattern,
                               test_name="AsperELISA")
        # BCult ------------------------------
        self.vprint("...processing Blood Culture results")
        df = pd.read_csv(self._get_path("BCult"), low_memory=False)
        sample_type_pattern = 'Specimen received:([\w\s\d\(\)\[\]\-]*)(Culture|Microscopy)'
        result_pattern = "(Culture|Microscopy-)([\w\s\d]*)"
        self._process_micro_df(df=df,
                               sample_type_pattern=sample_type_pattern,
                               result_pattern=result_pattern,
                               test_name="BloodCulture")
        # BGluc ------------------------------
        self.vprint("...processing Beta-Glucan results")
        df = pd.read_csv(self._get_path("BGluc"), low_memory=False)
        sample_type_pattern = 'Specimen received:([\w\s\d\(\)\[\]\-]*) Mycology reference unit'
        result_pattern = "Mycology reference unit Cardiff Beta Glucan Antigen Test :([\w\s\d<>/\.\-]*)"
        self._process_micro_df(df=df,
                               sample_type_pattern=sample_type_pattern,
                               result_pattern=result_pattern,
                               test_name="BetaGlucan")
        # RESPL ------------------------------
        self.vprint("...processing Respiratory Virus results")
        df = pd.read_csv(self._get_path("RESPL"), low_memory=False)
        sample_type_pattern = 'Specimen received:([\w\s\d<>/\.\-]*) (Microbiological ' \
                              'investigation of respiratory viruses|RESPL)'
        result_pattern = "(Microbiological investigation of respiratory viruses|RESPL)([\w\s\d<>/\.\-\(\):]*)"
        self._process_micro_df(df=df,
                               sample_type_pattern=sample_type_pattern,
                               result_pattern=result_pattern,
                               test_name="RESPL")

    def _comorbid(self):
        self.vprint("---- Populating Comorbid Table ----")
        for file in self.comorbid_files:
            df = pd.read_csv(self._get_path(file), low_memory=False)
            df.drop(["AGE", "GENDER", "ADMISSION_DATE", "TEST_DATE"], axis=1, inplace=True)
            df = _rename(df, additional_mappings={"SOLIDORGANTRANSPLANT": "solid_organ_transplant",
                                                  "CANCER": "cancer",
                                                  "SEVERERESPIRATORY": "severe_resp",
                                                  "SEVERESINGLEORGANDISEASE": "severe_single_organ_disease",
                                                  "RAREDISEASES": "rare_disease",
                                                  "IMMUNOSUPPRESSION": "immunosuppressed",
                                                  "PREGNANCYWITHCONGHEARTDIS": "pregnant_with_cong_heart_disease",
                                                  "GPIDENTIFIED_PATIENTS": "gp_identified_patients",
                                                  "RENAL_DIALYSIS": "renal_dialysis",
                                                  "OTHER": "other"})
            df.drop("request_location", axis=1, inplace=True)
            self._insert(df=df, table_name="Comorbid")

    def _haem(self):
        self.vprint("---- Populating ComplexHaematology Table ----")
        for file in progress_bar(self.haem_files, verbose=self.verbose):
            df = pd.read_csv(self._get_path(file), low_memory=False)
            df.drop(["AGE", "GENDER", "ADMISSION_DATE"], axis=1, inplace=True)
            df = _get_date_time(df)
            # pull out the sample type
            df["test_name"] = None
            df["test_result"] = None
            df["test_category"] = file
            df = _rename(df, {"TEXT": "raw_text"})
            self._insert(df=df, table_name="ComplexHaematology")

    def _covid_status(self, df: pd.DataFrame):
        covid = pd.read_csv(self._get_path("Covid19"), low_memory=False)
        covid_status = list()
        for pt_id in progress_bar(df.patient_id.unique(), verbose=self.verbose):
            pt_status = covid[covid.PATIENT_ID == pt_id].TEXT.values
            # No results, status is unknown
            if len(pt_status) == 0:
                covid_status.append("U")
                continue
            # If the patient was positive at any point,
            if any([x == "Positive" for x in pt_status]):
                covid_status.append("P")
            # If the patient has no positive results and not all tests are "In Progress", then register as negative
            elif all([x != "Positive" for x in pt_status]) and not all([x == "In Progress" for x in pt_status]):
                covid_status.append("N")
            else:
                # Otherwise status is unknown i.e. they are all "In Progress" with no "Positive" results
                covid_status.append("U")
        df["covid_status"] = covid_status
        return df

    def _register_death(self, df: pd.DataFrame):
        events = pd.read_csv(self._get_path("Outcomes"), low_memory=False)[["PATIENT_ID", "DESTINATION"]]
        death_status = list()
        for pt_id in progress_bar(df.patient_id.unique(), verbose=self.verbose):
            pt_events = events[events.PATIENT_ID == pt_id]
            pt_events = pt_events[pt_events.DESTINATION.isin(self.died_events)]
            if pt_events.shape[0] == 0:
                death_status.append(0)
            else:
                death_status.append(1)
        df["death"] = death_status
        return df

    def _patients(self):
        self.vprint("---- Populating Patients Table ----")
        self.vprint("...create basic table")
        df = pd.read_csv(self._get_path("People"), low_memory=False)
        df = df[df.TEST_PATIENT == "N"]
        df.drop("TEST_PATIENT", axis=1, inplace=True)
        df = _get_date_time(df, col_name="DATE_FROM", new_date_name="date_from", new_time_name="time_from")
        df = _get_date_time(df, col_name="DATE_ENTERED", new_date_name="date_entered", new_time_name="time_entered")
        df = df.rename({"PATIENT_ID": "patient_id",
                        "AGE": "age",
                        "GENDER": "gender"},
                       axis=1)
        self.vprint("...populate with COVID-19 status")
        df = self._covid_status(df)
        self.vprint("...populate with survival status")
        df = self._register_death(df)
        self._insert(df=df, table_name="Patients")

    def _critical_care(self):
        self.vprint("---- Populating Critical Care Table ----")
        df = pd.read_csv(self._get_path("CritCare"), low_memory=False)
        df.drop(["AGE", "GENDER", "ADMISSION_DATE", "TEST_DATE"], axis=1, inplace=True)
        df = _get_date_time(df, col_name="UNIT_ADMIT_DATE",
                            new_date_name="unit_admit_date",
                            new_time_name="unit_admit_time")
        df = _get_date_time(df, col_name="UNIT_DISCH_DATE",
                            new_date_name="unit_discharge_date",
                            new_time_name="unit_discharge_time")
        df = _rename(df, {"request_location": "location",
                          "ICU_DAY": "icu_length_of_stay",
                          "VENTILATOR": "ventilated",
                          "COVID19_STATUS": "covid_status"})
        self._insert(df=df, table_name="CritCare")

    def _radiology(self):
        self.vprint("---- Populate Radiology Table ----")
        self.vprint("....processing CT Angiogram pulmonary results")
        df = pd.read_csv(self._get_path("CTangio"), low_memory=False)
        df.drop(["AGE", "GENDER", "ADMISSION_DATE"], axis=1, inplace=True)
        df = _get_date_time(df, col_name="TEST_DATE",
                            new_date_name="test_date",
                            new_time_name="test_time")
        df["test_category"] = "CTangio"
        df = _rename(df, additional_mappings={"TEXT": "raw_text"})
        self.vprint("....processing X-ray results")
        df = pd.read_csv(self._get_path("XRChest"), low_memory=False)
        df.drop(["AGE", "GENDER", "ADMISSION_DATE"], axis=1, inplace=True)
        df = _get_date_time(df, col_name="TEST_DATE",
                            new_date_name="test_date",
                            new_time_name="test_time")
        df["test_category"] = "XRChest"
        df = _rename(df, additional_mappings={"TEXT": "raw_text"})
        self._insert(df=df, table_name="Radiology")

    def _events(self):
        self.vprint("---- Populate Events Table ----")
        df = pd.read_csv(self._get_path("Outcomes"), low_memory=False)
        df.drop(["WIMD", "GENDER"], axis=1, inplace=True)
        df = _get_date_time(df, col_name="EVENT_DATE",
                            new_date_name="event_date",
                            new_time_name="event_time")
        df["death"] = df.DESTINATION.apply(lambda x: int(any([i in str(x) for i in self.died_events])))
        df = df.rename({"PATIENT_ID": "patient_id",
                        "COMPONENT": "component",
                        "EVENT_TYPE": "event_type",
                        "COVID_STATUS": "covid_status",
                        "SOURCE_TYPE": "source_type",
                        "SOURCE": "source",
                        "DESTINATION": "destination",
                        "CRITICAL_CARE": "critical_care"}, axis=1)
        self._insert(df=df, table_name="Events")

    def populate(self):
        self.vprint("=============== Populating database ===============")
        self.vprint("\n")
        self._patients()
        self._comorbid()
        self._events()
        self._pathology()
        self._microbiology()
        self._radiology()
        self._critical_care()
        self._haem()
        self.vprint("\n")
        self.vprint("Complete!....")
        self.vprint("====================================================")

    def create_indexes(self):
        self.vprint("=============== Generating Indexes ===============")
        sql_statements = ["CREATE INDEX crit_care_pt_id ON CritCare (patient_id)",
                          "CREATE INDEX events_pt_id ON Events (patient_id)",
                          "CREATE INDEX radiology_pt_id ON Radiology (patient_id)",
                          "CREATE INDEX haem_pt_id ON ComplexHaematology (patient_id)",
                          "CREATE INDEX micro_pt_id ON Microbiology (patient_id)",
                          "CREATE INDEX path_pt_id ON Pathology (patient_id)",
                          "CREATE INDEX crit_care_ventilated ON CritCare (ventilated)",
                          "CREATE INDEX crit_care_covid ON CritCare (covid_status)",
                          "CREATE INDEX type_event ON Events (event_type)",
                          "CREATE INDEX covid_event ON Events (covid_status)",
                          "CREATE INDEX test_haem ON ComplexHaematology (test_category)",
                          "CREATE INDEX name_micro ON Microbiology (test_name)",
                          "CREATE INDEX result_micro ON Microbiology (test_result)",
                          "CREATE INDEX name_path ON Pathology (test_name)",
                          "CREATE INDEX cat_path ON Pathology (test_category)"]
        for x in progress_bar(sql_statements, verbose=self.verbose):
            self._curr.execute(x)
            self._connection.commit()
        self.vprint("\n")
        self.vprint("Complete!....")
        self.vprint("====================================================")

    def close(self):
        self._connection.close()