""" postgres common"""

import psycopg2
from sqlalchemy import create_engine
from datetime import datetime as dt

def write_pg(host: str, database:str, user: str, password: str,
            data, table: str = None, port:str="5432"):
    """"
     write data in pg
    """
    conn = psycopg2.connect(
        host=host,
        database=database,
        user=user,
        password=password,
        port = port
        )
    db_uri = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(db_uri)
    cur = conn.cursor()
    cur.execute(f"""SELECT EXISTS (SELECT FROM
        information_schema.tables WHERE table_name = '{table}');""")
    table_exists = cur.fetchone()[0]
    if not table_exists:
        if table == "motower_monthly":
            #delete_query = "DELETE FROM "+table+ " WHERE mois = %s;", (data.mois.unique()[0],)
            delete_query = f'DELETE FROM {table} WHERE mois = {data.mois.unique()[0]}'
            create_query = f"""
                CREATE TABLE {table} (
                id SERIAL PRIMARY KEY,
                mois VARCHAR,
                code_oci varchar,
                site VARCHAR,
                autre_code VARCHAR,
                longitude VARCHAR,
                latitude VARCHAR,
                type_du_site VARCHAR,
                statut VARCHAR,
                localisation VARCHAR,
                commune VARCHAR,
                departement VARCHAR,
                region VARCHAR,
                partenaires VARCHAR,
                proprietaire VARCHAR,
                gestionnaire VARCHAR,
                type_geolocalite VARCHAR,
                projet VARCHAR,
                clutter VARCHAR,
                position_site VARCHAR,
                ca_voix FLOAT,
                ca_data FLOAT,
                parc_global INTEGER,
                parc_data INTEGER,
                parc_2g INTEGER,
                parc_3g  INTEGER,
                parc_4g INTEGER,
                parc_5g INTEGER,
                autre_parc INTEGER,
                o_m FLOAT,
                energie FLOAT,
                infra FLOAT,
                maintenance_passive_preventive FLOAT,
                garde_de_securite FLOAT,
                discount FLOAT,
                volume_discount FLOAT,
                tva FLOAT,
                opex_itn FLOAT,
                delay_2g FLOAT,
                delay_3g FLOAT,
                delay_4g FLOAT,
                delaycellule_2g	FLOAT,
                delaycellule_3g	FLOAT,
                delaycellule_4g FLOAT,
                nbrecellule_2g INTEGER,
                nbrecellule_3g INTEGER,
                nbrecellule_4g INTEGER,
                trafic_voix_2g FLOAT,
                trafic_voix_3g FLOAT,
                trafic_voix_4g FLOAT,
                trafic_data_2g FLOAT,
                trafic_data_3g FLOAT,
                trafic_data_4g FLOAT,
                trafic_data_v2_2g  FLOAT, 
                trafic_data_v2_3g  FLOAT, 
                trafic_data_v2_4g  FLOAT, 
                trafic_voix_v2_2g  FLOAT, 
                trafic_voix_v2_3g  FLOAT, 
                trafic_voix_v2_4g  FLOAT,
                cellules_2g_congestionnees INTEGER,
                cellules_2g INTEGER,
                cellules_3g_congestionnees INTEGER,
                cellules_3g INTEGER,
                cellules_4g_congestionnees INTEGER,
                cellules_4g INTEGER,
                cellules_v2_2g  INTEGER, 
                cellules_congestionne_v2_2g INTEGER,
                cellules_v2_3g   INTEGER, 
                cellules_congestionne_v2_3g  INTEGER, 
                cellules_v2_4g   INTEGER, 
                cellules_congestionne_v2_4g  INTEGER,
                avg_cssr_cs_2g FLOAT,
                avg_cssr_cs_3g FLOAT,
                trafic_voix_total FLOAT,
                trafic_data_total FLOAT,
                ca_total FLOAT,
                segment VARCHAR,
                pareto BOOLEAN,
                cellules_congestionnees_total INTEGER,
                cellules_congestionnees_total_v2 INTEGER,
                cellules_total INTEGER,
                cellules_total_v2 INTEGER,
                taux_congestion_2g FLOAT,
                taux_congestion_3g FLOAT,
                taux_congestion_4g FLOAT,
                taux_congestion_total FLOAT,
                taux_congestion_2g_v2 FLOAT,
                taux_congestion_3g_v2 FLOAT,
                taux_congestion_4g_v2 FLOAT,
                taux_congestion_total_v2  FLOAT,
                recommandation VARCHAR,
                recommandation_v2  VARCHAR,
                arpu FLOAT,
                cssr_pondere_trafic_2g  FLOAT,
                cssr_pondere_trafic_3g FLOAT,
                cssr_pondere_trafic_2g_v2  FLOAT,
                cssr_pondere_trafic_3g_v2  FLOAT, 
                segmentation_rentabilite VARCHAR,
                segmentation_rentabilite_v2  VARCHAR,
                interco FLOAT,
                impot FLOAT,
                frais_dist FLOAT,
                opex FLOAT,
                autre_opex FLOAT,
                ebitda FLOAT,
                marge_ca FLOAT,
                rentable BOOLEAN,
                niveau_rentabilite VARCHAR,
                days INTEGER,
                nur_2g FLOAT,
                nur_3g FLOAT,
                nur_4g FLOAT,
                nur_total FLOAT,
                nur_2g_v2  FLOAT,
                nur_3g_v2  FLOAT,
                nur_4g_v2  FLOAT,
                nur_total_v2  FLOAT,
                previous_segment VARCHAR
            );
            """
        
        if table == "motower_daily":
            delete_query = f'DELETE FROM {table} WHERE jour = {data.jour.unique()[0]}'
            create_query = f"""
                CREATE TABLE {table} (
                id SERIAL PRIMARY KEY,
                jour date,
                code_oci varchar,
                autre_code VARCHAR,
                clutter VARCHAR,
                commune VARCHAR,
                departement VARCHAR,
                type_du_site VARCHAR,
                type_geolocalite VARCHAR,
                gestionnaire VARCHAR,
                latitude VARCHAR,
                longitude VARCHAR,
                localisation VARCHAR,
                partenaires VARCHAR,
                proprietaire VARCHAR,
                position_site VARCHAR,
                site          VARCHAR,
                statut VARCHAR,
                projet VARCHAR, 
                region VARCHAR,
                ca_data FLOAT,            
                ca_voix FLOAT,
                ca_total FLOAT,
                parc_global INTEGER,
                parc_data INTEGER,
                parc_2g INTEGER,
                parc_3g  INTEGER,
                parc_4g INTEGER,
                parc_5g INTEGER,
                autre_parc INTEGER,
                trafic_data_in_ko  FLOAT,
                trafic_voix_in_minute FLOAT               
            );
            """
        

        cur.execute(create_query)
    cur.execute(delete_query)
    conn.commit()
    cur.close()
    conn.close()

    data.to_sql(table, engine, index=False, if_exists = 'append')
