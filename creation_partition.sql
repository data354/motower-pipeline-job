  
--MOTOWER DAILY CAPARC_dimensions

-- PARTITION PAR JOUR 

CREATE TABLE public.motower_daily_caparc_dimension (
    id serial4 NOT NULL,
    jour date NOT NULL,
    code_oci varchar NULL,
    code_oci_id float8 NULL,
    autre_code varchar NULL,
    clutter varchar NULL,
    commune varchar NULL,
    departement varchar NULL,
    type_du_site varchar NULL,
    type_geolocalite varchar NULL,
    gestionnaire varchar NULL,
    latitude varchar NULL,
    longitude varchar NULL,
    localisation varchar NULL,
    partenaires varchar NULL,
    proprietaire varchar NULL,
    position_site varchar NULL,
    site varchar NULL,
    statut varchar NULL,
    projet varchar NULL,
    region varchar NULL,
    segment varchar NULL,
    previous_segment varchar NULL,
    evolution_segment int4 NULL,
    CONSTRAINT motower_daily_caparc_dimension_pkey PRIMARY KEY (id, jour)
) PARTITION BY RANGE (jour);



DO $$ 
DECLARE
    date_record DATE;
BEGIN 
    FOR date_record IN 
        SELECT d::date
        FROM generate_series('2024-06-01'::date, '2024-06-30'::date, '1 day'::interval) AS d
    LOOP
        EXECUTE format(
            'CREATE TABLE public.motower_daily_caparc_dimension_%s PARTITION OF public.motower_daily_caparc_dimension
             FOR VALUES FROM (''%s'') TO (''%s'')',
            to_char(date_record, 'YYYYMMDD'),
            date_record,
            date_record + interval '1 day'
        );
    END LOOP;
END $$;
 
--#################################################################################################################################
--##############################################################################################################################

--MOTOWER CA PARC FAIT 

--PARTITION PAR MOIS ET PAR JOUR 


CREATE TABLE public.motower_daily_caparc_faits (
    id serial4 NOT NULL,
    jour date NOT NULL,
    code_oci varchar NULL,
    code_oci_id float8 NULL,
    mois int4 NOT NULL,
    annee int4 NOT NULL,
    ca_data float8 NULL,
    ca_voix float8 NULL,
    ca_total float8 NULL,
    parc_global int4 NULL,
    parc_data int4 NULL,
    parc_2g int4 NULL,
    parc_3g int4 NULL,
    parc_4g int4 NULL,
    parc_5g int4 NULL,
    autre_parc int4 NULL,
    trafic_data_in float8 NULL,
    trafic_voix_in float8 NULL,
    trafic_data_in_mo float8 NULL,
    ca_mtd float8 NULL,
    ca_norm float8 NULL,
    CONSTRAINT motower_daily_caparc_faits_pkey PRIMARY KEY (id, mois, jour)
) PARTITION BY RANGE (mois);



CREATE OR REPLACE FUNCTION create_monthly_and_daily_partitions(year INTEGER) RETURNS VOID AS $$
DECLARE
    month INTEGER;
    start_month INTEGER;
    end_month INTEGER;
    monthly_partition_name TEXT;
    daily_partition_name TEXT;
    date_record DATE;
BEGIN
    start_month := 1;
    end_month := 12;

    FOR month IN start_month..end_month LOOP
        monthly_partition_name := format('public.motower_daily_caparc_faits_%s_%02s', year, month);

        -- Create monthly partitions that are also partitioned by jour
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I PARTITION OF public.motower_daily_caparc_faits
            FOR VALUES FROM (%s) TO (%s)
            PARTITION BY RANGE (jour);',
            monthly_partition_name, month, month + 1);

        -- Create daily partitions within the monthly partition
        FOR date_record IN 
            SELECT d::date
            FROM generate_series(
                (year || '-' || to_char(month, 'FM00') || '-01')::date, 
                (year || '-' || to_char(month, 'FM00') || '-01')::date + INTERVAL '1 month' - INTERVAL '1 day', 
                '1 day'::interval) AS d
        LOOP
            daily_partition_name := format('%s_%s', monthly_partition_name, to_char(date_record, 'YYYYMMDD'));
            EXECUTE format('
                CREATE TABLE IF NOT EXISTS %I PARTITION OF %I
                FOR VALUES FROM (''%s'') TO (''%s'');',
                daily_partition_name,
                monthly_partition_name,
                date_record, 
                date_record + INTERVAL '1 day');
        END LOOP;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


SELECT create_monthly_and_daily_partitions(2024);

--##################################################################################################################################################
--################################################################################################################################################

--MOTOWER CONGESTION

--PARTITION PAR MOIS ET PAR JOUR 

CREATE TABLE public.motower_daily_congestion (
    id serial4 NOT NULL,
    jour date NOT NULL,
    mois int4 NOT NULL,
    annee int4 NOT NULL,
    id_site float8 NOT NULL,
    cellules_2g int4 NULL,
    cellules_3g int4 NULL,
    cellules_4g int4 NULL,
    cellules_2g_congestionnees int4 NULL,
    cellules_3g_congestionnees int4 NULL,
    cellules_4g_congestionnees int4 NULL,
    cellules_totales int4 NULL,
    cellules_congestionnees_totales int4 NULL,
    CONSTRAINT motower_daily_congestion_pkey PRIMARY KEY (id, mois, jour)
) PARTITION BY RANGE (mois);


CREATE OR REPLACE FUNCTION create_monthly_and_daily_partitions_congestion(year INTEGER) RETURNS VOID AS $$
DECLARE
    month INTEGER;
    start_month INTEGER;
    end_month INTEGER;
    monthly_partition_name TEXT;
    daily_partition_name TEXT;
    date_record DATE;
BEGIN
    start_month := 1;
    end_month := 12;

    FOR month IN start_month..end_month LOOP
        monthly_partition_name := format('public.motower_daily_congestion_%s_%02s', year, month);

        -- Create monthly partitions that are also partitioned by jour
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I PARTITION OF public.motower_daily_congestion
            FOR VALUES FROM (%s) TO (%s)
            PARTITION BY RANGE (jour);',
            monthly_partition_name, month, month + 1);

        -- Create daily partitions within the monthly partition
        FOR date_record IN 
            SELECT d::date
            FROM generate_series(
                (year || '-' || to_char(month, 'FM00') || '-01')::date, 
                (year || '-' || to_char(month, 'FM00') || '-01')::date + INTERVAL '1 month' - INTERVAL '1 day', 
                '1 day'::interval) AS d
        LOOP
            daily_partition_name := format('%s_%s', monthly_partition_name, to_char(date_record, 'YYYYMMDD'));
            EXECUTE format('
                CREATE TABLE IF NOT EXISTS %I PARTITION OF %I
                FOR VALUES FROM (''%s'') TO (''%s'');',
                daily_partition_name,
                monthly_partition_name,
                date_record, 
                date_record + INTERVAL '1 day');
        END LOOP;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


SELECT create_monthly_and_daily_partitions_congestion(2024);


--########################################################################################################################################################
--#######################################################################################################################################################


--MOTOWER TRAFIC 
--PARTITION PAR MOIS ET PAR JOUR 

CREATE TABLE public.motower_daily_trafic (
    id serial4 NOT NULL,
    jour date NOT NULL,
    mois int4 NOT NULL,
    annee int4 NOT NULL,
    id_site float8 NOT NULL,
    trafic_data_2g float8 NULL,
    trafic_data_3g float8 NULL,
    trafic_data_4g float8 NULL,
    trafic_voix_2g float8 NULL,
    trafic_voix_3g float8 NULL,
    trafic_voix_4g float8 NULL,
    trafic_voix_total float8 NULL,
    trafic_data_total float8 NULL,
    CONSTRAINT motower_daily_trafic_pkey PRIMARY KEY (id, mois, jour)
) PARTITION BY RANGE (mois);



CREATE OR REPLACE FUNCTION create_monthly_and_daily_partitions_trafic(year INTEGER) RETURNS VOID AS $$
DECLARE
    month INTEGER;
    start_month INTEGER;
    end_month INTEGER;
    monthly_partition_name TEXT;
    daily_partition_name TEXT;
    date_record DATE;
BEGIN
    start_month := 1;
    end_month := 12;

    FOR month IN start_month..end_month LOOP
        monthly_partition_name := format('public.motower_daily_trafic_%s_%02s', year, month);

        -- Create monthly partitions that are also partitioned by jour
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I PARTITION OF public.motower_daily_trafic
            FOR VALUES FROM (%s) TO (%s)
            PARTITION BY RANGE (jour);',
            monthly_partition_name, month, month + 1);

        -- Create daily partitions within the monthly partition
        FOR date_record IN 
            SELECT d::date
            FROM generate_series(
                (year || '-' || to_char(month, 'FM00') || '-01')::date, 
                (year || '-' || to_char(month, 'FM00') || '-01')::date + INTERVAL '1 month' - INTERVAL '1 day', 
                '1 day'::interval) AS d
        LOOP
            daily_partition_name := format('%s_%s', monthly_partition_name, to_char(date_record, 'YYYYMMDD'));
            EXECUTE format('
                CREATE TABLE IF NOT EXISTS %I PARTITION OF %I
                FOR VALUES FROM (''%s'') TO (''%s'');',
                daily_partition_name,
                monthly_partition_name,
                date_record, 
                date_record + INTERVAL '1 day');
        END LOOP;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


SELECT create_monthly_and_daily_partitions_trafic(2024);





