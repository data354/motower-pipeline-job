  
MOTOWER DAILY CAPARC_dimensions

PARTITION PAR JOUR 

CREATE TABLE public.motower_daily_caparc_dimension (
    id serial4 NOT NULL,
    jour date NOT NULL,
    mois int4 NOT NULL,
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
    CONSTRAINT motower_daily_caparc_dimension_pkey PRIMARY KEY (id, jour,mois)
) PARTITION BY RANGE (mois);



CREATE OR REPLACE FUNCTION create_dimension_partition(year INTEGER) RETURNS VOID AS 
$$
DECLARE
    month INTEGER;
    start_month INTEGER :=1;
    end_month INTEGER :=12;
    monthly_partition_name TEXT;
BEGIN
    FOR month IN start_month..end_month LOOP
        monthly_partition_name := format('public.motower_daily_caparc_dimension_%s_%02s',
        year,lpad(month::text,2,'0'));

      EXECUTE format(
      'CREATE TABLE IF NOT EXISTS %s 
      	PARTITION OF public.motower_daily_caparc_dimension
        FOR VALUES FROM (%s) TO (%s);',monthly_partition_name, month, month + 1       
      );
    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT create_dimension_partition(2024);
 
#################################################################################################################################
##############################################################################################################################

MOTOWER CA PARC FAIT 

PARTITION PAR MOIS ET PAR JOUR 


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
    segment varchar NULL,
    previous_segment varchar NULL,
    evolution_segment int4 NULL,
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

##################################################################################################################################################
################################################################################################################################################

MOTOWER CONGESTION

PARTITION PAR MOIS ET PAR JOUR 

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


########################################################################################################################################################
#######################################################################################################################################################


MOTOWER TRAFIC 
PARTITION PAR MOIS ET PAR JOUR 

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




##############################################################################################################################################
#############################################################################################################################################

// MOTOWER VUE

CREATE OR REPLACE VIEW public.motower_daily_caparc
AS SELECT md.jour,
    md.code_oci,
    md.code_oci_id,
    md.autre_code,
    md.clutter,
    md.commune,
    md.departement,
    md.type_du_site,
    md.type_geolocalite,
    md.gestionnaire,
    md.latitude,
    md.longitude,
    md.localisation,
    md.partenaires,
    md.proprietaire,
    md.position_site,
    md.site,
    md.statut,
    md.projet,
    md.region,
    mf.segment,
    mf.previous_segment,
    mf.evolution_segment,
    mf.ca_voix,
    mf.ca_data,
    mf.ca_total,
    mf.parc_global,
    mf.parc_data,
    mf.parc_2g,
    mf.parc_3g,
    mf.parc_4g,
    mf.parc_5g,
    mf.autre_parc,
    mf.trafic_data_in,
    mf.trafic_voix_in,
    mf.trafic_data_in_mo,
    mf.ca_mtd,
    mf.ca_norm
   FROM motower_daily_caparc_dimension md
     JOIN motower_daily_caparc_faits mf ON md.jour = mf.jour AND md.code_oci::text = mf.code_oci::text AND md.code_oci_id = mf.code_oci_id;



-----------------------------------------------------------------------------------------
NEW TABLE 
----------------------------------------------------------------------------------------

-- Supprimer la table actuelle (si elle existe) avant de la recréer
DROP TABLE IF EXISTS public.motower_daily_caparc_faits_new;

CREATE TABLE public.motower_daily_caparc_faits_new (
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
    segment varchar NULL,
    previous_segment varchar NULL,
    evolution_segment int4 NULL,
    CONSTRAINT motower_daily_caparc_faits_new_pkey PRIMARY KEY (id, annee, mois)
) PARTITION BY RANGE (annee, mois);



CREATE OR REPLACE FUNCTION create_faits_monthly_partitions(year INTEGER) RETURNS VOID AS $$
DECLARE
    month INTEGER;
    next_year INTEGER;
    next_month INTEGER;
    monthly_partition_name TEXT;
BEGIN
    -- Boucle sur chaque mois de l'année
    FOR month IN 1..12 LOOP
        -- Calculer le mois suivant
        next_month := month + 1;
        IF next_month > 12 THEN
            next_month := 1;  -- Retour à janvier
            next_year := year + 1;  -- Passage à l'année suivante
        ELSE
            next_year := year;  -- Toujours la même année si ce n'est pas décembre
        END IF;

        -- Nom de la partition mensuelle (par année et mois)
        monthly_partition_name := format('public.motower_daily_caparc_faits_%s_%02s', year, month);

        -- Créer la partition mensuelle sous la partition annuelle existante
        EXECUTE format('
            CREATE TABLE IF NOT EXISTS %I PARTITION OF public.motower_daily_caparc_faits_new
            FOR VALUES FROM (%s, %s) TO (%s, %s);',
            monthly_partition_name, 
            year, month,  -- Début du mois
            next_year, next_month  -- Mois suivant, avec gestion du passage à l'année suivante
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;












MOTOWER DAILY CAPARC_dimensions

PARTITION PAR JOUR 

CREATE TABLE public.motower_daily_caparc_dimension_new (
    id serial4 NOT NULL,
    jour date NOT NULL,
    mois int4 NOT NULL,
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
    CONSTRAINT motower_daily_caparc_dimension_new_pkey PRIMARY KEY (id, jour,mois)
) PARTITION BY RANGE (mois);



CREATE OR REPLACE FUNCTION create_dimension_partition_new(year INTEGER) RETURNS VOID AS 
$$
DECLARE
    month INTEGER;
    start_month INTEGER :=1;
    end_month INTEGER :=12;
    monthly_partition_name TEXT;
BEGIN
    FOR month IN start_month..end_month LOOP
        monthly_partition_name := format('public.motower_daily_caparc_dimension_%s_%02s',
        year,lpad(month::text,2,'0'));

      EXECUTE format(
      'CREATE TABLE IF NOT EXISTS %s 
      	PARTITION OF public.motower_daily_caparc_dimension_new
        FOR VALUES FROM (%s) TO (%s);',monthly_partition_name, month, month + 1       
      );
    END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT create_dimension_partition_new(2025);



SELECT create_faits_monthly_partitions(2025);

