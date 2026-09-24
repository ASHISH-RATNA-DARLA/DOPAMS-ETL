import re

with open('etl-ir/ir_etl.py', 'r') as f:
    content = f.read()

# Replace insert_main_record and insert_related_records entirely.
# They span from `def insert_main_record` until `def run(self):`.
new_insert = """
    def insert_main_record(self, record: Dict[str, Any], cursor, is_update: bool = False):
        pf = record.get('PHYSICAL_FEATURES', {})
        sep = record.get('SOCIO_ECONOMIC_PROFILE', {})
        coo = record.get('COMMISSION_OF_OFFENCE', {})
        soas = record.get('SHARE_OF_AMOUNT_SPENT', {})
        pw = record.get('PRESENT_WHEREABOUTS', {})
        
        in_jail = pw.get('IN_JAIL', {})
        on_bail = pw.get('ON_BAIL', {})
        absconding = pw.get('ABSCONDING', {})
        normal_life = pw.get('NORMAL_LIFE', {})
        rehabilitated = pw.get('REHABILITATED', {})
        dead = pw.get('DEAD', {})
        facing_trial = pw.get('FACING_TRIAL', {})
        
        lang_dialect = pf.get('LANGUAGE_OR_DIALECT', [])
        if not isinstance(lang_dialect, list):
            lang_dialect = []
            
        ir_id = record.get('INTERROGATION_REPORT_ID')
        
        # Insert Media into file_media_bookkeeping
        media = record.get('MEDIA', [])
        if media:
            now_utc = datetime.now(timezone.utc)
            media_values = [
                (ir_id, idx, media_id, SOURCE_SYSTEM, SOURCE_ENDPOINT, now_utc, ETL_RUN_ID)
                for idx, media_id in enumerate(media) if media_id
            ]
            if media_values:
                from psycopg2.extras import execute_values
                execute_values(
                    cursor,
                    \"\"\"INSERT INTO file_media_bookkeeping
                           (parent_id, file_index, file_id, source_system, source_endpoint, fetched_at, etl_run_id,
                            source_type, source_field)
                       VALUES %s ON CONFLICT DO NOTHING\"\"\",
                    media_values,
                    template="(%s, %s, %s::uuid, %s, %s, %s, %s, 'interrogation', 'MEDIA')"
                )
        
        main_values = (
            ir_id,
            record.get('CRIME_ID'),
            normalize_person_id(record.get('PERSON_ID')),
            pf.get('BEARD'),
            pf.get('BUILD'),
            pf.get('BURN_MARKS'),
            pf.get('COLOR'),
            pf.get('DEFORMITIES_OR_PECULIARITIES'),
            pf.get('DEFORMITIES'),
            pf.get('EAR'),
            pf.get('EYES'),
            pf.get('FACE'),
            pf.get('HAIR'),
            pf.get('HEIGHT'),
            pf.get('IDENTIFICATION_MARKS'),
            lang_dialect,
            pf.get('LEUCODERMA'),
            pf.get('MOLE'),
            pf.get('MUSTACHE'),
            pf.get('NOSE'),
            pf.get('SCAR'),
            pf.get('TATTOO'),
            pf.get('TEETH'),
            
            sep.get('LIVING_STATUS'),
            sep.get('MARITAL_STATUS'),
            sep.get('EDUCATION'),
            sep.get('OCCUPATION'),
            sep.get('INCOME_GROUP'),
            
            coo.get('OFFENCE_TIME'),
            coo.get('OTHER_OFFENCE_TIME'),
            
            soas.get('SHARE_OF_AMOUNT_SPENT'),
            soas.get('OTHER_SHARE_OF_AMOUNT_SPENT'),
            soas.get('REMARKS'),
            
            in_jail.get('IS_IN_JAIL'),
            in_jail.get('FROM_WHERE_SENT'),
            in_jail.get('CRIME_NUM'),
            in_jail.get('DIST_UNIT'),
            
            on_bail.get('IS_ON_BAIL'),
            on_bail.get('FROM_WHERE_SENT'),
            on_bail.get('CRIME_NUM'),
            parse_iso_date(on_bail.get('DATE_OF_BAIL')) if on_bail.get('DATE_OF_BAIL') else None,
            
            absconding.get('IS_ABSCONDING'),
            absconding.get('WANTED_IN_POLICE_STATION'),
            absconding.get('CRIME_NUM'),
            
            normal_life.get('IS_NORMAL_LIFE'),
            normal_life.get('EKING_LIVELIHOOD_BY_LABOR_WORK'),
            
            rehabilitated.get('IS_REHABILITATED'),
            rehabilitated.get('REHABILITATION_DETAILS'),
            
            dead.get('IS_DEAD'),
            dead.get('DEATH_DETAILS'),
            
            facing_trial.get('IS_FACING_TRIAL'),
            facing_trial.get('PS_NAME'),
            facing_trial.get('CRIME_NUM'),
            
            record.get('OTHER_REGULAR_HABITS'),
            record.get('OTHER_INDULGENCE_BEFORE_OFFENCE') if record.get('OTHER_INDULGENCE_BEFORE_OFFENCE') is not None else record.get('OTHER_INDULGANCE_BEFORE_OFFENCE'),
            record.get('TIME_SINCE_MODUS_OPERANDI'),
            parse_timestamp(record.get('DATE_CREATED')),
            parse_timestamp(record.get('DATE_MODIFIED')),
            SOURCE_SYSTEM,
            SOURCE_ENDPOINT,
            datetime.now(timezone.utc),
            ETL_RUN_ID,
            # JSONB child arrays
            json.dumps(record.get('ASSOCIATE_DETAILS', [])),
            json.dumps(record.get('CONSUMER_DETAILS', [])),
            json.dumps(record.get('CONVICTION_ACQUITTAL', [])),
            json.dumps(record.get('DEFENCE_COUNSEL', [])),
            json.dumps(record.get('DOPAMS_LINKS', [])),
            json.dumps(record.get('EXECUTION_OF_NBW', [])),
            json.dumps(record.get('FAMILY_HISTORY', [])),
            json.dumps(record.get('FINANCIAL_HISTORY', [])),
            json.dumps(record.get('INDULGANCE_BEFORE_OFFENCE', [])),
            json.dumps(record.get('INTERROGATION_REPORT', [])),
            json.dumps(record.get('JAIL_SENTENCE', [])),
            json.dumps(record.get('LOCAL_CONTACTS', [])),
            json.dumps(record.get('MODUS_OPERANDI', [])),
            json.dumps(record.get('NEW_GANG_FORMATION', [])),
            json.dumps(record.get('PENDING_NBW', [])),
            json.dumps(record.get('PREVIOUS_OFFENCES_CONFESSED', [])),
            json.dumps(record.get('PROPERTY_DISPOSAL', [])),
            json.dumps(record.get('REGULAR_HABITS', [])),
            json.dumps(record.get('REGULARIZATION_OF_TRANSIT_WARRANTS', [])),
            json.dumps(record.get('SHELTER', [])),
            json.dumps(record.get('SIM_DETAILS', [])),
            json.dumps(record.get('SURETIES', [])),
            json.dumps(record.get('TYPES_OF_DRUGS', []))
        )

        query = f\"\"\"
            INSERT INTO {IR_TABLE} (
                interrogation_report_id, crime_id, person_id,
                physical_beard, physical_build, physical_burn_marks, physical_color,
                physical_deformities_or_peculiarities, physical_deformities, physical_ear,
                physical_eyes, physical_face, physical_hair, physical_height,
                physical_identification_marks, physical_language_or_dialect,
                physical_leucoderma, physical_mole, physical_mustache, physical_nose,
                physical_scar, physical_tattoo, physical_teeth,
                
                socio_living_status, socio_marital_status, socio_education,
                socio_occupation, socio_income_group,
                
                offence_time, other_offence_time,
                share_of_amount_spent, other_share_of_amount_spent, share_remarks,
                
                is_in_jail, from_where_sent_in_jail, in_jail_crime_num, in_jail_dist_unit,
                
                is_on_bail, from_where_sent_on_bail, on_bail_crime_num, date_of_bail,
                
                is_absconding, wanted_in_police_station, absconding_crime_num,
                
                is_normal_life, eking_livelihood_by_labor_work,
                
                is_rehabilitated, rehabilitation_details,
                
                is_dead, death_details,
                
                is_facing_trial, facing_trial_ps_name, facing_trial_crime_num,
                
                other_regular_habits, other_indulgence_before_offence,
                time_since_modus_operandi,
                date_created, date_modified, source_system, source_endpoint, fetched_at, etl_run_id,
                
                associate_details, consumer_details, conviction_acquittal, defence_counsel, dopams_links,
                execution_of_nbw, family_history, financial_history, indulgance_before_offence,
                interrogation_report_refs, jail_sentence, local_contacts, modus_operandi,
                new_gang_formation, pending_nbw, previous_offences_confessed, property_disposal,
                regular_habits, regularization_transit_warrants, shelter, sim_details, sureties, types_of_drugs
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s,
                %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb,
                %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb,
                %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb
            )
            ON CONFLICT (interrogation_report_id) DO UPDATE SET
                crime_id = EXCLUDED.crime_id,
                person_id = EXCLUDED.person_id,
                physical_beard = EXCLUDED.physical_beard,
                physical_build = EXCLUDED.physical_build,
                physical_burn_marks = EXCLUDED.physical_burn_marks,
                physical_color = EXCLUDED.physical_color,
                physical_deformities_or_peculiarities = EXCLUDED.physical_deformities_or_peculiarities,
                physical_deformities = EXCLUDED.physical_deformities,
                physical_ear = EXCLUDED.physical_ear,
                physical_eyes = EXCLUDED.physical_eyes,
                physical_face = EXCLUDED.physical_face,
                physical_hair = EXCLUDED.physical_hair,
                physical_height = EXCLUDED.physical_height,
                physical_identification_marks = EXCLUDED.physical_identification_marks,
                physical_language_or_dialect = EXCLUDED.physical_language_or_dialect,
                physical_leucoderma = EXCLUDED.physical_leucoderma,
                physical_mole = EXCLUDED.physical_mole,
                physical_mustache = EXCLUDED.physical_mustache,
                physical_nose = EXCLUDED.physical_nose,
                physical_scar = EXCLUDED.physical_scar,
                physical_tattoo = EXCLUDED.physical_tattoo,
                physical_teeth = EXCLUDED.physical_teeth,
                
                socio_living_status = EXCLUDED.socio_living_status,
                socio_marital_status = EXCLUDED.socio_marital_status,
                socio_education = EXCLUDED.socio_education,
                socio_occupation = EXCLUDED.socio_occupation,
                socio_income_group = EXCLUDED.socio_income_group,
                
                offence_time = EXCLUDED.offence_time,
                other_offence_time = EXCLUDED.other_offence_time,
                share_of_amount_spent = EXCLUDED.share_of_amount_spent,
                other_share_of_amount_spent = EXCLUDED.other_share_of_amount_spent,
                share_remarks = EXCLUDED.share_remarks,
                
                is_in_jail = EXCLUDED.is_in_jail,
                from_where_sent_in_jail = EXCLUDED.from_where_sent_in_jail,
                in_jail_crime_num = EXCLUDED.in_jail_crime_num,
                in_jail_dist_unit = EXCLUDED.in_jail_dist_unit,
                
                is_on_bail = EXCLUDED.is_on_bail,
                from_where_sent_on_bail = EXCLUDED.from_where_sent_on_bail,
                on_bail_crime_num = EXCLUDED.on_bail_crime_num,
                date_of_bail = EXCLUDED.date_of_bail,
                
                is_absconding = EXCLUDED.is_absconding,
                wanted_in_police_station = EXCLUDED.wanted_in_police_station,
                absconding_crime_num = EXCLUDED.absconding_crime_num,
                
                is_normal_life = EXCLUDED.is_normal_life,
                eking_livelihood_by_labor_work = EXCLUDED.eking_livelihood_by_labor_work,
                
                is_rehabilitated = EXCLUDED.is_rehabilitated,
                rehabilitation_details = EXCLUDED.rehabilitation_details,
                
                is_dead = EXCLUDED.is_dead,
                death_details = EXCLUDED.death_details,
                
                is_facing_trial = EXCLUDED.is_facing_trial,
                facing_trial_ps_name = EXCLUDED.facing_trial_ps_name,
                facing_trial_crime_num = EXCLUDED.facing_trial_crime_num,
                
                other_regular_habits = EXCLUDED.other_regular_habits,
                other_indulgence_before_offence = EXCLUDED.other_indulgence_before_offence,
                time_since_modus_operandi = EXCLUDED.time_since_modus_operandi,
                date_created = EXCLUDED.date_created,
                date_modified = EXCLUDED.date_modified,
                source_system = EXCLUDED.source_system,
                source_endpoint = EXCLUDED.source_endpoint,
                fetched_at = EXCLUDED.fetched_at,
                etl_run_id = EXCLUDED.etl_run_id,
                
                associate_details = EXCLUDED.associate_details,
                consumer_details = EXCLUDED.consumer_details,
                conviction_acquittal = EXCLUDED.conviction_acquittal,
                defence_counsel = EXCLUDED.defence_counsel,
                dopams_links = EXCLUDED.dopams_links,
                execution_of_nbw = EXCLUDED.execution_of_nbw,
                family_history = EXCLUDED.family_history,
                financial_history = EXCLUDED.financial_history,
                indulgance_before_offence = EXCLUDED.indulgance_before_offence,
                interrogation_report_refs = EXCLUDED.interrogation_report_refs,
                jail_sentence = EXCLUDED.jail_sentence,
                local_contacts = EXCLUDED.local_contacts,
                modus_operandi = EXCLUDED.modus_operandi,
                new_gang_formation = EXCLUDED.new_gang_formation,
                pending_nbw = EXCLUDED.pending_nbw,
                previous_offences_confessed = EXCLUDED.previous_offences_confessed,
                property_disposal = EXCLUDED.property_disposal,
                regular_habits = EXCLUDED.regular_habits,
                regularization_transit_warrants = EXCLUDED.regularization_transit_warrants,
                shelter = EXCLUDED.shelter,
                sim_details = EXCLUDED.sim_details,
                sureties = EXCLUDED.sureties,
                types_of_drugs = EXCLUDED.types_of_drugs
        \"\"\"
        cursor.execute(query, main_values)
        return True

    def run(self):
"""

content = re.sub(r"    def insert_main_record\(self, record: Dict\[str, Any\], cursor, is_update: bool = False\):.*?    def run\(self\):", new_insert, content, flags=re.DOTALL)

with open('etl-ir/ir_etl.py', 'w') as f:
    f.write(content)
print("Done inserting new logic!")
