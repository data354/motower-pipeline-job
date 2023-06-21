""" postgres common"""

import psycopg2

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
    cur = conn.cursor()
    cur.execute(f"""SELECT EXISTS (SELECT FROM
        information_schema.tables WHERE table_name = '{table}');""")
    table_exists = cur.fetchone()[0]
    if not table_exists:
        create_query = f"""
            CREATE TABLE {table} (
            id SERIAL PRIMARY KEY,
            mois VARCHAR,
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
            o_m FLOAT,
            energy FLOAT,
            infra FLOAT,
            maintenance_passive_preventive FLOAT,
            gardes_de_securite FLOAT,
            discount FLOAT,
            volume_discount FLOAT,
            tva FLOAT,
            opex FLOAT,
            delay_2g FLOAT,
            delay_3g FLOAT,
            delay_4g FLOAT,
            nbrecellule_2g INTEGER,
            nbrecellule_3g INTEGER,
            nbrecellule_4g INTEGER,
            trafic_voix_2g FLOAT,
            trafic_voix_3g FLOAT,
            trafic_voix_4g FLOAT,
            trafic_data_2g FLOAT,
            trafic_data_3g FLOAT,
            trafic_data_4g FLOAT,
            avg_cssr_cs_2g FLOAT,
            avg_cssr_cs_3g FLOAT,
            trafic_voix_total FLOAT,
            trafic_data_total FLOAT,
            ca_total FLOAT,
            segment VARCHAR,
            pareto BOOLEAN,
            date DATE,
            interco FLOAT,
            impot FLOAT,
            frais_dist FLOAT,
            opex_total FLOAT,
            ebitda FLOAT,
            rentable BOOLEAN,
            niveau_rentabilite BOOLEAN,
            cellules_2g_congestionnees INTEGER,
            cellules_3g_congestionnees INTEGER,
            cellules_4g_congestionnees INTEGER,
            cellules_2g INTEGER,
            cellules_3g INTEGER,
            cellules_4g INTEGER,
            days INTEGER,
            nur_2g FLOAT,
            nur_3g FLOAT,
            nur_4g FLOAT,
            previous_segment VARCHAR
        );
        """
        cur.execute(create_query)
    cur.execute("DELETE FROM "+table+ "WHERE mois = %s;", (data.mois.unique()[0],))
    conn.commit()
    cur.close()
    data.to_sql(table, conn, index=False, if_exists = 'append')
    conn.close()
