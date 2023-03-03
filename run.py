# %%

from utils import create_db_connection
import pandas as pd
from omegaconf import OmegaConf

## yaml 파일 및 biosignal_db_schema를 설정해주세요.
biosignal_config = OmegaConf.load("biosignal_config.yaml")
cdm_config = OmegaConf.load("cdm_config.yaml")
biosignal_db_schema = ".".join([biosignal_config.database, biosignal_config.schema])
cdm_db_schema = ".".join([cdm_config.database, cdm_config.schema])
cdm_person_table_name = "cdm_patno.csv"  # cdm 아이디 변환을 위한 매핑 테이블 경로

wave_mapping_dict = {
    "SPO2IRAC": 4155650,
    "RESPIMP": 4081054,
    "ECGII": 4168140,
    "CVP": 4313586,
    "ART1": 4301474,
    "ICP1": 4082375,
}
# %%


def run():
    # biosignal 서버 및 CDM 서버의 접속 정보를 적어주시면 됩니다.
    biosignal_conn = create_db_connection(
        driver=biosignal_config.driver,
        server=biosignal_config.server,
        database=biosignal_config.database,
        port=biosignal_config.port,
        username=biosignal_config.username,
        password=biosignal_config.password,
    )
    cdm_conn = create_db_connection(
        driver=cdm_config.driver,
        server=cdm_config.server,
        database=cdm_config.database,
        port=cdm_config.port,
        username=cdm_config.username,
        password=cdm_config.password,
    )

    # 원본 환자번호를 CDM 환자번호로 교체
    cdm_person_table = pd.read_csv(cdm_person_table_name)
    cdm_person_dict = dict(
        zip(cdm_person_table["patno"], cdm_person_table["cdm_patno"])
    )

    print("Extract biosignal data...")
    extract_sqls = f"""select A.patient_id, B.starttime, B.endtime, B.wavetype, B.filepath
    from {biosignal_db_schema}.patientid_mapping A, {biosignal_db_schema}.waveform_info B
    where A.anonymous_id = B.patient_id ;
    """
    biosignal_data = pd.read_sql(extract_sqls, biosignal_conn)

    # Change original person_id to cdm_person_id
    biosignal_data["patient_id"] = biosignal_data["patient_id"].map(cdm_person_dict)
    biosignal_data["patient_id"] = biosignal_data.patient_id.astype(
        "Int64", errors="ignore"
    )
    biosignal_data["starttime"] = pd.to_datetime(biosignal_data.starttime)
    biosignal_data["endtime"] = pd.to_datetime(biosignal_data.endtime)
    biosignal_data["concept_id"] = biosignal_data.wavetype.map(wave_mapping_dict)
    biosignal_data = biosignal_data.dropna(subset=["concept_id"])

    print("Insert meta table...")
    biosignal_data.to_sql(
        "biosignal_meta", schema=cdm_config.schema, if_exists="replace", con=cdm_conn
    )

    print("Insert biosignal information to CDM observation table...")
    insert_query = f"""
    INSERT INTO {cdm_db_schema}.observation
    (observation_id, person_id, observation_concept_id, observation_date,observation_datetime, observation_type_concept_id,value_as_number,
    value_as_string,value_as_concept_id ,qualifier_concept_id ,unit_concept_id ,provider_id ,visit_occurrence_id ,
    visit_detail_id ,observation_source_value,observation_source_concept_id ,unit_source_value ,qualifier_source_value )
    select
    (SELECT MAX(observation_id) FROM {cdm_db_schema}.observation) + ROW_NUMBER() OVER (ORDER BY m.patient_id) AS observation_id,
        m.patient_id as person_id,
        m.concept_id as observation_concept_id,
        CONVERT(date, m.starttime) as observation_date,
        CONVERT(datetime, m.starttime) as observation_datetime,
        5001 as observation_type_concept_id,
        0 as value_as_number, -- 0으로 부탁드립니다.
        0 as value_as_string, -- 0으로 부탁드립니다.
        0 as value_as_concept_id,
        NULL as qualifier_concept_id, -- null로 부탁드립니다.
        NULL as unit_concept_id, -- null로 부탁드립니다.
        NULL as provider_id, -- CDM 기존 변환 방식과 동일하게 채워주시길 부탁드립니다.
        NULL as visit_occurrence_id, -- CDM 기존 변환 방식과 동일하게 채워주시길 부탁드립니다.
        NULL as visit_detail_id, -- CDM 기존 변환 방식과 동일하게 채워주시길 부탁드립니다.
        m.wavetype as observation_source_value,
        NULL as observation_source_concept_id, -- 0으로 부탁드립니다.
        NULL as unit_source_value, -- null로 부탁드립니다.
        NULL as qualifier_value -- null로 부탁드립니다.
    FROM {cdm_db_schema}.biosignal_meta_short m
    where m.patient_id is not null;     
    """
    with cdm_conn.connect() as connection:
        connection.execute(insert_query)

    print("Done!")


if __name__ == "__main__":
    run()

# %%
